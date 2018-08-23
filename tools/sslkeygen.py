import os
import os.path
import subprocess
import tempfile

from dtests.dtest import debug, keystore_type_desc, trusttore_type_desc


def generate_credentials(ip, cakeystore=None, cacert=None):

    tmpdir = tempfile.mkdtemp()

    if not cakeystore:
        cakeystore = generate_cakeypair(tmpdir, 'ca')
    if not cacert:
        cacert = generate_cert(tmpdir, "ca", cakeystore)

    # create keystore with new private key
    name = "ip" + ip
    jkeystore = generate_ipkeypair(tmpdir, name, ip)

    # create signed cert
    csr = generate_sign_request(tmpdir, name, jkeystore, ['-ext', 'san=ip:' + ip])
    cert = sign_request(tmpdir, "ca", cakeystore, csr, ['-ext', 'san=ip:' + ip])

    # import cert chain into keystore
    import_cert(tmpdir, "ca", cacert, jkeystore)
    import_cert(tmpdir, name, cert, jkeystore)

    return SecurityCredentials(jkeystore, cert, cakeystore, cacert)


def generate_cakeypair(dir, name):
    return generate_keypair(dir, name, name, ['-ext', 'bc:c'])


def generate_ipkeypair(dir, name, ip):
    return generate_keypair(dir, name, ip, ['-ext', 'san=ip:' + ip])


def generate_dnskeypair(dir, name, hostname):
    return generate_keypair(dir, name, hostname, ['-ext', 'san=dns:' + hostname])


def generate_keypair(dir, name, cn, opts):
    kspath = os.path.join(dir, name + '.keystore')
    return _exec_keytool(dir, kspath, ['-alias', name, '-genkeypair', '-keyalg', 'RSA', '-dname',
                                       "cn={}, ou=cassandra, o=apache.org, c=US".format(cn), '-keypass', 'cassandra'] + opts)


def generate_cert(dir, name, keystore, opts=None):
    if opts is None:
        opts = []
    fn = os.path.join(dir, name + '.pem')
    _exec_keytool(dir, keystore, ['-alias', name, '-exportcert', '-rfc', '-file', fn] + opts)
    return fn


def generate_sign_request(dir, name, keystore, opts=None):
    if opts is None:
        opts = []
    fn = os.path.join(dir, name + '.csr')
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-certreq', '-file', fn] + opts)
    return fn


def sign_request(dir, name, keystore, csr, opts=None):
    if opts is None:
        opts = []
    fnout = os.path.splitext(csr)[0] + '.pem'
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-gencert',
                                  '-rfc', '-infile', csr, '-outfile', fnout] + opts)
    return fnout


def import_cert(dir, name, cert, keystore, opts=None):
    if opts is None:
        opts = []
    _exec_keytool(dir, keystore, ['-alias', name, '-keypass', 'cassandra', '-importcert', '-noprompt', '-file', cert] + opts)
    return cert


def _exec_keytool(dir, keystore, opts):
    args = ['keytool', '-keystore', keystore, '-storepass', 'cassandra', '-deststoretype', 'pkcs12'] + opts
    subprocess.check_call(args)
    return keystore


class SecurityCredentials():

    def __init__(self, keystore, cert, cakeystore, cacert):
        self.keystore = keystore
        self.cert = cert
        self.cakeystore = cakeystore
        self.cacert = cacert
        self.basedir = os.path.dirname(self.keystore)

    def __str__(self):
        return "keystore: {}, cert: {}, cakeystore: {}, cacert: {}".format(
               self.keystore, self.cert, self.cakeystore, self.cacert)


def generate_ssl_stores(base_dir, passphrase='cassandra'):
    """
    Util for generating ssl stores using java keytool and openssl -- nondestructive method if stores already exist this
    method is a no-op.

    @param base_dir (str) directory where keystore.p12, truststore.p12 (jks for dse versions < 7), ccm_node.cer and ccm_node.key will be placed
    @param passphrase (Optional[str]) currently ccm expects a passphrase of 'cassandra' so it's the default but it can
    be overridden for failure testing
    @return None
    @throws CalledProcessError If the keytool or openssl fail during any step
    """

    keystore_desc = keystore_type_desc()
    truststore_desc = trusttore_type_desc()

    if os.path.exists(os.path.join(base_dir, keystore_desc.getFileName())):
        debug("keystores already exists - skipping generation of ssl keystores")
        return

    debug("generating {0} in [{1}]".format(keystore_desc.getFileName(), base_dir))
    subprocess.check_call(['keytool', '-genkeypair', '-alias', 'ccm_node', '-keyalg', 'RSA', '-validity', '365',
                           '-keystore', os.path.join(base_dir, keystore_desc.getFileName()), '-storepass', passphrase,
                           '-storetype', keystore_desc.getType(),
                           '-dname', 'cn=Cassandra Node,ou=CCMnode,o=DataStax,c=US', '-keypass', passphrase])
    debug("exporting cert from {0} in [{1}]".format(keystore_desc.getFileName(), base_dir))
    subprocess.check_call(['keytool', '-export', '-rfc', '-alias', 'ccm_node',
                           '-keystore', os.path.join(base_dir, keystore_desc.getFileName()),
                           '-file', os.path.join(base_dir, 'ccm_node.cer'), '-storepass', passphrase])
    debug("importing cert into {0} in [{1}]".format(truststore_desc.getFileName(), base_dir))
    subprocess.check_call(['keytool', '-import', '-file', os.path.join(base_dir, 'ccm_node.cer'),
                           '-alias', 'ccm_node', '-keystore', os.path.join(base_dir, truststore_desc.getFileName()),
                           '-storetype', truststore_desc.getType(),
                           '-storepass', passphrase, '-noprompt'])
    debug("exporting {0} into {0} in [{1}]".format(keystore_desc.getFileName(), base_dir))
    subprocess.check_call(['keytool', '-importkeystore', '-srckeystore', os.path.join(base_dir, keystore_desc.getFileName()),
                           '-srcstorepass', passphrase, '-srcalias', 'ccm_node', '-destalias', 'ccm_node',
                           '-deststorepass', passphrase, '-destkeystore', os.path.join(base_dir, keystore_desc.getFileName()),
                           '-deststoretype', keystore_desc.getType()])
    debug("exporting key from {0} into [{1}]".format(keystore_desc.getFileName(), base_dir))
    subprocess.check_call(['openssl', 'pkcs12', '-in', os.path.join(base_dir, keystore_desc.getFileName()), '-passin',
                           "pass:%s" % passphrase, '-nodes', '-nocerts', '-out',
                           os.path.join(base_dir, 'ccm_node.key')])
