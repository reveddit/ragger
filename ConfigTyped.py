import configparser


class ConfigTyped():
    def __init__(self, configFile, mode):
        config = configparser.ConfigParser()
        config.read(configFile)
        opts = {}
        for g in ['default', mode]:
            for o in config.options(g):
                opts[o] = config.get(g, o)
                if opts[o].isdigit():
                    opts[o] = config.getint(g, o)
                elif opts[o].lower() in ['true', 'false']:
                    opts[o] = config.getboolean(g, o)
        self.opts = opts

    def get_connectString(self, dbname):
        return ('postgresql://'+
                self.opts['db_user']+':'+
                self.opts['db_pw']+'@localhost:'+
                str(self.opts['db_port'])+'/'+
                dbname)

    def get_connectString_psy(self, dbname):
        return (f"""
            host='localhost' dbname='{dbname}' user='{self.opts['db_user']}' password='{self.opts['db_pw']}' port='{str(self.opts['db_port'])}'
        """)
