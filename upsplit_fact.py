import re
from sqlalchemy import Column, Date, DateTime, Float, Integer, LargeBinary, Numeric, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
metadata = Base.metadata

class FactElement(Base):
    __tablename__ = 'fact_element_local_name'

    element_local_name_id = Column(Integer, primary_key=True)
    fact_id = Column(Integer, nullable=False)
    element_local_name = Column(String, nullable=False)

sql_engine = create_engine("mssql+pymssql://admin:Welcome@1@./edgar_db")

metadata.bind = sql_engine
for table in metadata.sorted_tables:
    table.create(sql_engine, checkfirst=True)

Session = sessionmaker(bind=sql_engine)

with sql_engine.connect() as sql_conn:

    count = 100000
    errored_out = []
    i = 0

    session = Session()

    while i < count:
        reports = []
        try:
            r_rows = sql_conn.execute(f'SELECT report_id FROM dbo.report ORDER BY report_id DESC OFFSET {i} ROWS FETCH NEXT 1000 ROWS ONLY;')
            for r_row in r_rows:
                reports.append(r_row.report_id)
        except:
            continue
        else:
            for report in reports:
                f_rows = sql_conn.execute(f'SELECT fact_id, element_local_name FROM dbo.fact_newest WHERE accession_id = {report} AND element_local_name IS NOT NULL;')
                for f_row in f_rows:
                    el_list = []
                    el_list = re.findall('[A-Z][^A-Z]*', f_row.element_local_name)
                    for el in el_list:
                        session.add(FactElement(fact_id=f_row.fact_id, element_local_name=el))        
                try:
                    session.commit()
                except Exception as e:
                    errored_out.append(report)
                    print(report)
                    print(f"Error: {e}")
                    session.rollback()

                print(f"facts for report {report} splitted")

            i += 1000

    session.close()
    print(errored_out)
    
    sql_conn.close()