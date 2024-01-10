import sqlalchemy


class UsersSFMC_TblSchema:
    def __init__(self):
        schema_dict = {
            "email": {"sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                    "pandas": "object"},
            "name": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                "pandas": "object",
            },
            "surname": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                "pandas": "object",
            },
            "mobilephone": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                "pandas": "object",
            },
            "lng": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                "pandas": "object",
            },
            "countryCode": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                "pandas": "object",
            },
            "data_extension": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                "pandas": "object",
            },
             "registry_date": {
                "sqlalchemy": sqlalchemy.types.DateTime,
                "pandas": "object",
            },
             "last_update": {
                "sqlalchemy": sqlalchemy.types.DateTime,
                "pandas": "object",
            }
            
        }
        self.pandas_dtypes = {k: schema_dict[k]["pandas"] for k in schema_dict.keys()}
        self.sqlalchemy_dtypes = {k: schema_dict[k]["sqlalchemy"] for k in schema_dict.keys()}

class UsersOKTA_TblSchema:
    def __init__(self):
        schema_dict = {
            "id": {"sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                    "pandas": "object"},
            "email": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                "pandas": "object",
            },
            "account_type": {
                "sqlalchemy": sqlalchemy.types.VARCHAR(length=50),
                "pandas": "object",
            },
            "withdrawl": {
                "sqlalchemy": sqlalchemy.types.Integer,
                "pandas": "object",
            },
             "last_update": {
                "sqlalchemy": sqlalchemy.types.DateTime,
                "pandas": "object",
            }
        }
        self.pandas_dtypes = {k: schema_dict[k]["pandas"] for k in schema_dict.keys()}
        self.sqlalchemy_dtypes = {k: schema_dict[k]["sqlalchemy"] for k in schema_dict.keys()}

class OneTrustConsents_TblSchema:
    def __init__(self):
        schema_dict = {
            "email": {"sqlalchemy": sqlalchemy.types.VARCHAR(length=255),
                    "pandas": "object"},
            "mars_petcare_consent": {
                "sqlalchemy": sqlalchemy.types.Integer,
                "pandas": "object",
            },
            "rc_mkt_consent": {
                "sqlalchemy": sqlalchemy.types.Integer,
                "pandas": "object",
            },
            "data_research_consent": {
                "sqlalchemy": sqlalchemy.types.Integer,
                "pandas": "object",
            },
            "rc_tyc_consent": {
                "sqlalchemy": sqlalchemy.types.Integer,
                "pandas": "object",
            }
            
        }
        self.pandas_dtypes = {k: schema_dict[k]["pandas"] for k in schema_dict.keys()}
        self.sqlalchemy_dtypes = {k: schema_dict[k]["sqlalchemy"] for k in schema_dict.keys()}
