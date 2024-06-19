from sqlalchemy import Column, Integer, LargeBinary, Numeric, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
metadata = Base.metadata


class Fact(Base):
    __tablename__ = 'fact'

    fact_id = Column(Integer, primary_key=True)
    accession_id = Column(Integer, nullable=False)
    context_id = Column(Integer)
    unit_id = Column(Integer)
    unit_base_id = Column(Integer)
    element_id = Column(Integer, nullable=False)
    fact_value = Column(String)
    xml_id = Column(String)
    precision_value = Column(Integer)
    decimals_value = Column(Integer)
    is_precision_infinity = Column(Boolean, nullable=False)
    is_decimals_infinity = Column(Boolean, nullable=False)
    ultimus_index = Column(Integer)
    calendar_ultimus_index = Column(Integer)
    fiscal_ultimus_index = Column(Integer)
    uom = Column(String)
    is_extended = Column(Boolean)
    fiscal_year = Column(Integer)
    fiscal_period = Column(String)
    calendar_year = Column(Integer)
    calendar_period = Column(String)
    tuple_fact_id = Column(Integer)
    fact_hash = Column(LargeBinary)
    calendar_hash = Column(LargeBinary)
    fiscal_hash = Column(LargeBinary)
    entity_id = Column(Integer)
    element_namespace = Column(String)
    element_local_name = Column(String)
    dimension_count = Column(Integer)
    inline_display_value = Column(String)
    inline_scale = Column(Integer)
    inline_negated = Column(Boolean)
    inline_is_hidden = Column(Boolean)
    inline_format_qname_id = Column(Integer)



# params = urllib.parse.quote_plus('Driver={ODBC Driver 17 for SQL Server};' 'Server=.;'
#                                  'Database=edgar_db;' 'UID=admin;' 'PWD=Welcome@1;')
sql_engine = create_engine("mssql+pymssql://admin:Welcome@1@./edgar_db")

psql_engine = create_engine("postgresql://xuspwr164:tOmh4nYXSGBFA4@public.xbrl.us:5432/edgar_db")

metadata.bind = sql_engine
for table in metadata.sorted_tables:
    table.create(sql_engine, checkfirst=True)

Session = sessionmaker(bind=sql_engine)

with open(f"error_log.txt", "a") as error_log:
    
    with psql_engine.connect() as psql_conn:
    
        with sql_engine.connect() as sql_conn:
       
            # rep_count = 0
            errored_out = []
            i = 313141
            # count = sql_conn.execute(f'SELECT count(1) as count FROM dbo.report;')
            # for row in count:
            #     rep_count = row['count']

            while i > 0:
                r_rows = sql_conn.execute(f'SELECT report_id FROM dbo.report ORDER BY report_id DESC OFFSET {i} ROWS FETCH NEXT 1000 ROWS ONLY;')
                for row in r_rows:
                 
                try:
                    facts = psql_conn.execute(f'SELECT fact_id, accession_id, context_id, unit_id, unit_base_id, element_id, fact_value, xml_id, precision_value, decimals_value, is_precision_infinity, is_decimals_infinity, ultimus_index, calendar_ultimus_index, fiscal_ultimus_index, uom, is_extended, fiscal_year, fiscal_period, calendar_year, calendar_period, tuple_fact_id, fact_hash, calendar_hash, fiscal_hash, entity_id, element_namespace, element_local_name, dimension_count, inline_display_value, inline_scale, inline_negated, inline_is_hidden, inline_format_qname_id FROM public.fact WHERE accession_id = {i};')
                except:
                    error_log.write(f"report_id = {i} Error: {e}")
                    errored_out.append(i)
                else:
                    session = Session()
                    for row in facts:              
                        session.add(Fact(**row))

                    try:
                        session.commit()
                    except Exception as e:
                        error_log.write(f"report_id = {i} Error: {e}")
                        errored_out.append(i)
                        session.rollback()
                    finally:
                        print(f'rows for report id {i} downloaded')
                        session.close()
                finally:
                    i -= 1000

            print(errored_out)
        
            sql_conn.close()

        psql_conn.close()
        
    error_log.close()