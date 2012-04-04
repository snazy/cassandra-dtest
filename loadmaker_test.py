import time

import loadmaker

from dtest import Tester, debug

class TestLoadmaker(Tester):
    
    def loadmaker_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        time.sleep(.2)
        cursor = self.cql_connection(node1).cursor()

        lm = loadmaker.LoadMaker(cursor, column_family_name='cf_standard',
                consistency_level='ONE')
        lm.generate(500)
        lm.validate()
        lm.update(100)
        lm.validate()
        lm.delete(10)
        lm.validate()

        lm = loadmaker.LoadMaker(cursor, column_family_name='cf_counter',
                is_counter=True,
                consistency_level='ONE')
        lm.generate(200)
        lm.validate()


        lm1 = loadmaker.LoadMaker(cursor, column_family_name='cf_standard2',
                consistency_level='ONE')
        lm2 = loadmaker.LoadMaker(cursor, column_family_name='cf_counter2',
                is_counter=True,
                consistency_level='ONE')

        cont_loader = loadmaker.ContinuousLoader([lm1, lm2])
        time.sleep(10)
        cont_loader.read_and_validate()

        cluster.cleanup()
