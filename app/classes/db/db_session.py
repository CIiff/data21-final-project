import sqlalchemy as sa
import sqlalchemy.orm as orm
from sqlalchemy.orm import Session

from app.classes.db.modelbase import SqlAlchemyBase

__factory = None


def global_init(db_str: str, db_name):
    """
    creates connection engine from the input database connection string
    and creates a new database if the db_name is not already in the server
    """
    global __factory

    if __factory:
        return
    
    engine = sa.create_engine(db_str, echo=False, 
                            connect_args={"check_same_thread": False})
    
    connection = engine.connect()

    # get list of databases in server
    dbs = engine.execute('SELECT name FROM sys.databases')
    dbs = [d[0] for d in dbs]

    # check if db name not in list of datbases
    if db_name not in dbs:
        engine.execute(f"""
                        CREATE DATABASE {db_name};
                        """)
    
    # change connection to new database
    engine.execute(f'USE {db_name}')

    # binds engine to session, to make transactions to sql server
    __factory = orm.sessionmaker(bind=engine)

    # import all table models
    import app.classes.db.__all_models

    # create all tables based on models
    SqlAlchemyBase.metadata.create_all(engine)


# useful for single sql inserts, may be overrules by pd.df.to_sql
def create_session() -> Session:
    global __factory

    session = __factory()

    session.expire_on_commit = False

    return session