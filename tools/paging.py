import time
import traceback

from dse.cluster import ContinuousPagingOptions
from tools.datahelp import flatten_into_set
from dtest import debug

from threading import Condition, Thread


class Page(object):
    data = None

    def __init__(self):
        self.data = []

    def add_row(self, row):
        self.data.append(row)


class PageFetcher(object):
    """
    Requests pages, handles their receipt,
    and provides paged data for testing.

    The first page is automatically retrieved, so an initial
    call to request_one is actually getting the *second* page!
    """
    pages = None
    error = None
    future = None
    requested_pages = None
    retrieved_pages = None
    retrieved_empty_pages = None

    def __init__(self, future):
        self.pages = []

        # the first page is automagically returned (eventually)
        # so we'll count this as a request, but the retrieved count
        # won't be incremented until it actually arrives
        self.requested_pages = 1
        self.retrieved_pages = 0
        self.retrieved_empty_pages = 0

        self.future = future
        self.future.add_callbacks(
            callback=self.handle_page,
            errback=self.handle_error
        )

        # wait for the first page to arrive, otherwise we may call
        # future.has_more_pages too early, since it should only be
        # called after the first page is returned
        self.wait(seconds=30)

    def handle_page(self, rows):
        # occasionally get a final blank page that is useless
        if not rows:
            self.retrieved_empty_pages += 1
            return

        page = Page()
        self.pages.append(page)

        for row in rows:
            page.add_row(row)

        self.retrieved_pages += 1

    def handle_error(self, exc):
        self.error = exc
        raise exc

    def request_one(self, timeout=None):
        """
        Requests the next page if there is one.

        If the future is exhausted, this is a no-op.
        @param timeout Time, in seconds, to wait for all pages.
        """
        if self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def request_all(self, timeout=None):
        """
        Requests any remaining pages.

        If the future is exhausted, this is a no-op.
        @param timeout Time, in seconds, to wait for all pages.
        """
        while self.future.has_more_pages:
            self.future.start_fetching_next_page()
            self.requested_pages += 1
            self.wait(seconds=timeout)

        return self

    def wait(self, seconds=None):
        """
        Blocks until all *requested* pages have been returned.

        Requests are made by calling request_one and/or request_all.

        Raises RuntimeError if seconds is exceeded.
        """
        seconds = 5 if seconds is None else seconds
        expiry = time.time() + seconds

        while time.time() < expiry:
            if self.requested_pages == (self.retrieved_pages + self.retrieved_empty_pages):
                return self
            # small wait so we don't need excess cpu to keep checking
            time.sleep(0.1)

        raise RuntimeError(
            "Requested pages were not delivered before timeout. " +
            "Requested: {}; retrieved: {}; empty retrieved: {}".format(self.requested_pages, self.retrieved_pages, self.retrieved_empty_pages))

    def pagecount(self):
        """
        Returns count of *retrieved* pages which were not empty.

        Pages are retrieved by requesting them with request_one and/or request_all.
        """
        return len(self.pages)

    def num_results(self, page_num):
        """
        Returns the number of results found at page_num
        """
        return len(self.pages[page_num - 1].data)

    def num_results_all(self):
        return [len(page.data) for page in self.pages]

    def page_data(self, page_num):
        """
        Returns retreived data found at pagenum.

        The page should have already been requested with request_one and/or request_all.
        """
        return self.pages[page_num - 1].data

    def all_data(self):
        """
        Returns all retrieved data flattened into a single list (instead of separated into Page objects).

        The page(s) should have already been requested with request_one and/or request_all.
        """
        all_pages_combined = []
        for page in self.pages:
            all_pages_combined.extend(page.data[:])

        return all_pages_combined

    @property  # make property to match python driver api
    def has_more_pages(self):
        """
        Returns bool indicating if there are any pages not retrieved.
        """
        return self.future.has_more_pages


class ContinuousPageFetcher(object):
    """
    The page fetcher for a continuous paging session.

    The driver currently pushes a row generator to the callback, this generator
    can potentially block, therefore it must be iterated in a separate thread.
    WARNING: the final API is subject to change.

    In order to run existing paging tests with continuous paging enabled, therefore
    exercising the same code server side, this page fetcher will emulate pages
    by assuming that each page has page_size rows, except for the last one if the
    page unit is in ROWS. If the page unit is BYTES, it estimates the number of rows
    by dividing the page size by the row size in bytes, but this is not really accurate
    because the server can send a page up to (page_size - avg_row_size) early, or slightly
    over, depending on how close rows are to their average size.
    """
    def __init__(self, future, page_size, page_unit=ContinuousPagingOptions.PagingUnit.ROWS, row_size_bytes=None):
        self.rows = []
        self.page_size = page_size
        self.page_unit = page_unit
        self.row_size_bytes = row_size_bytes
        self._condition = Condition()
        self.all_fetched = False

        self.error = None
        self.future = future
        self.future.add_callbacks(
            callback=self.handle_rows,
            errback=self.handle_error
        )

    def handle_rows(self, rows):

        def inner_handle_rows():
            try:
                for row in rows:
                    # debug("Row {}".format(row))
                    self.rows.append(row)

                self.notify_finished(True)
            except Exception, ex:
                self.handle_error(ex)

        thread = Thread(target=inner_handle_rows)
        thread.start()

    def handle_error(self, exc):
        debug("Received error {}".format(exc))
        traceback.print_exc()
        self.error = exc
        self.notify_finished(False)

    def notify_finished(self, all_fetched):
        with self._condition:
            self.all_fetched = all_fetched
            self._condition.notify()

    def wait(self, timeout=None):
        """
        Blocks until all rows have been received.
        """
        timeout = 5 if timeout is None else timeout
        # debug('Waiting for up to {} seconds'.format(timeout))
        with self._condition:
            self._condition.wait(timeout)

        if self.error:
            raise self.error

        if not self.all_fetched:
            raise RuntimeError("Failed to receive all rows before timeout, only got {} rows.".format(len(self.rows)))

        return self

    def _totalSize(self):
        """
        :return: the total size received, expressed in page units (either bytes or rows)
        """
        if self.page_unit == ContinuousPagingOptions.PagingUnit.BYTES:
            return len(self.rows) * self.row_size_bytes
        elif self.page_unit == ContinuousPagingOptions.PagingUnit.ROWS:
            return len(self.rows)
        else:
            raise AssertionError('Unsupported page unit {}'.format(self.page_unit))

    def _sizeInRows(self, size):
        """
        :return: the size expressed in rows
        """
        if self.page_unit == ContinuousPagingOptions.PagingUnit.BYTES:
            return size / self.row_size_bytes
        elif self.page_unit == ContinuousPagingOptions.PagingUnit.ROWS:
            return size
        else:
            raise AssertionError('Unsupported page unit {}'.format(self.page_unit))

    def pagecount(self):
        """
        Returns count of *retrieved* pages which were not empty.

        Pages are retrieved by requesting them with request_one and/or request_all.
        """
        total_size = self._totalSize()
        return total_size / self.page_size + (1 if total_size % self.page_size > 0 else 0)

    def num_results(self, page_num):
        """
        Returns the number of results found at page_num, which starts at one so we
        must subtract one.
        """
        num_pages = self.pagecount()
        remainder = self._totalSize() % self.page_size
        page_num -= 1
        if 0 <= page_num < num_pages or (page_num == num_pages and remainder == 0):
            return self._sizeInRows(self.page_size)
        elif page_num == num_pages:
            return self._sizeInRows(remainder)
        else:
            return 0

    def num_results_all(self):
        num_pages = self._totalSize() / self.page_size
        ret = [self._sizeInRows(self.page_size) for _ in xrange(num_pages)]

        remainder = self._totalSize() % self.page_size
        if remainder > 0:
            ret.append(self._sizeInRows(remainder))

        return ret

    def page_data(self, page_num):
        """
        Returns retrieved data found at page_num, we assume we have a full page except for
        the last page. page_num starts at one for the first page so we must subtract one
        """
        rows_per_page = self._sizeInRows(self.page_size)
        start_idx = (page_num - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, len(self.rows))
        return self.rows[start_idx:end_idx]

    def all_data(self):
        """
        Returns all retrieved rows.
        """
        return self.rows

    @property
    def has_more_pages(self):
        """
        Returns a bool indicating if there are any pages not retrieved
        """
        return not self.all_fetched


class PageAssertionMixin(object):
    """Can be added to subclasses of unittest.Tester"""

    def assertEqualIgnoreOrder(self, actual, expected):
        return self.assertItemsEqual(actual, expected)

    def assertIsSubsetOf(self, subset, superset):
        self.assertLessEqual(flatten_into_set(subset), flatten_into_set(superset))
