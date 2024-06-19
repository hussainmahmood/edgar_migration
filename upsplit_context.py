import re
from sqlalchemy import Column, Date, DateTime, Float, Integer, LargeBinary, Numeric, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
metadata = Base.metadata

class ContextDimension(Base):
    __tablename__ = 'context_dimension_local_name'

    dimension_local_name_id = Column(Integer, primary_key=True)
    context_dimension_id = Column(Integer, nullable=False)
    dimension_local_name = Column(String, nullable=False)

class ContextMember(Base):
    __tablename__ = 'context_member_local_name'

    member_local_name_id = Column(Integer, primary_key=True)
    context_dimension_id = Column(Integer, nullable=False)
    member_local_name = Column(String, nullable=False)


sql_engine = create_engine("mssql+pymssql://admin:Welcome@1@./edgar_db")

metadata.bind = sql_engine
for table in metadata.sorted_tables:
    table.create(sql_engine, checkfirst=True)

Session = sessionmaker(bind=sql_engine)

with sql_engine.connect() as sql_conn:

    count = 30000000
    errored_out = []
    i = 0

    session = Session()

    while i < count:
        try:
            c_rows = sql_conn.execute(f'SELECT context_dimension_id, dimension_local_name, member_local_name FROM dbo.context_dimension_explicit_new ORDER BY context_dimension_id DESC OFFSET {i} ROWS FETCH NEXT 1000 ROWS ONLY;')
        except:
            continue
        else:
            for c_row in c_rows:
                dl_list = []
                if c_row.dimension_local_name:
                    dl_list = re.findall('[A-Z][^A-Z]*', c_row.dimension_local_name)
                    for dl in dl_list:
                        session.add(ContextDimension(context_dimension_id=c_row.context_dimension_id, dimension_local_name=dl))

                ml_list = []
                if c_row.member_local_name:
                    ml_list = re.findall('[A-Z][^A-Z]*', c_row.member_local_name)
                    for ml in ml_list:
                        session.add(ContextMember(context_dimension_id=c_row.context_dimension_id, member_local_name=ml))
                    
            try:
                session.commit()
            except Exception as e:
                errored_out.append(i)
                print(i)
                print(f"Error: {e}")
                session.rollback()

            i += 1000
            print(f"{i} context_dimension_explicit rows splitted")
    
    session.close()
    print(errored_out)

    sql_conn.close()