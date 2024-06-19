from sqlalchemy import Column, Date, DateTime, Float, Integer, LargeBinary, Numeric, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
metadata = Base.metadata


class ContextDimensionExplicit(Base):
    __tablename__ = 'context_dimension_explicit_new'

    context_dimension_id = Column(Integer, primary_key=True)
    context_id = Column(Integer, nullable=False)
    dimension_qname_id = Column(Integer, nullable=False)
    member_qname_id = Column(Integer)
    typed_qname_id = Column(Integer)
    is_default = Column(Boolean, nullable=False)
    is_segment = Column(Boolean)
    typed_text_content = Column(String)
    dimension_namespace = Column(String)
    dimension_local_name = Column(String)
    member_namespace = Column(String)
    member_local_name = Column(String)



# params = urllib.parse.quote_plus('Driver={ODBC Driver 17 for SQL Server};' 'Server=.;'
#                                  'Database=edgar_db;' 'UID=admin;' 'PWD=Welcome@1;')
sql_engine = create_engine("mssql+pymssql://admin:Welcome@1@./edgar_db")

psql_engine = create_engine("postgresql://xuspwr164:tOmh4nYXSGBFA4@public.xbrl.us:5432/edgar_db")

metadata.bind = sql_engine
for table in metadata.sorted_tables:
    table.create(sql_engine, checkfirst=True)

Session = sessionmaker(bind=sql_engine)

with open(f"context_error_log.txt", "a") as error_log:
    
    with psql_engine.connect() as psql_conn:
    
        with sql_engine.connect() as sql_conn:
       
            rep_count = 4950000
            errored_out = []
            i = 4900000
            # count = sql_conn.execute(f'SELECT count(1) as count FROM dbo.report;')
            # for row in count:
            #     rep_count = row['count']

            session = Session()

            while i < rep_count:
                contexts = []
                context_ids = ""
                try:
                    c_rows = sql_conn.execute(f'SELECT context_id FROM dbo.context_new ORDER BY context_id DESC OFFSET {i} ROWS FETCH NEXT 10000 ROWS ONLY;')
                except:
                    continue
                else:
                    for c_row in c_rows:
                        if c_row.context_id <= 56182918 and  c_row.context_id >= 56172919:
                            contexts.append(str(c_row.context_id))

                    if len(contexts):
                        context_ids = ", ".join(contexts)

                        try:
                            cde_rows = psql_conn.execute(f'SELECT * FROM public.context_dimension_explicit WHERE context_id IN ({context_ids});')
                            for cde_row in cde_rows:              
                                session.add(ContextDimensionExplicit(**cde_row))
                            try:
                                session.commit()
                            except Exception as e:
                                error_log.write(f"context_id = {contexts[0]} Error: {e}\n")
                                errored_out.append(contexts[0])
                                session.rollback()

                        except Exception as e:
                            error_log.write(f"context_id = {contexts[0]} Error: {e}\n")
                            errored_out.append(contexts[0])
                        
                        print(f'rows for context id {contexts[0]}-{contexts[len(contexts)-1]} downloaded')

                    i += 10000
                    print(i)
            
            session.close()
            print(errored_out)
        
            sql_conn.close()

        psql_conn.close()
        
    error_log.close()