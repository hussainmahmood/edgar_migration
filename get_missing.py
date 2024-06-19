from sqlalchemy import Column, Date, DateTime, Float, Integer, LargeBinary, Numeric, String, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
metadata = Base.metadata


class Context(Base):
    __tablename__ = 'context_new'

    context_id = Column(Integer, primary_key=True)
    accession_id = Column(Integer, nullable=False)
    period_start = Column(Date)
    period_end = Column(Date)
    period_instant = Column(Date)
    specifies_dimensions = Column(Boolean, nullable=False)
    context_xml_id = Column(String, nullable=False)
    entity_scheme = Column(String, nullable=False)
    entity_identifier = Column(String, nullable=False)
    fiscal_year = Column(Integer)
    fiscal_period = Column(String)
    context_hash = Column(LargeBinary)
    dimension_hash = Column(LargeBinary)
    calendar_year = Column(Integer)
    calendar_period = Column(String)
    calendar_start_offset = Column(Numeric(38, 10))
    calendar_end_offset = Column(Numeric(38, 10))
    calendar_period_size_diff_percentage = Column(Float(53))
    dimension_count = Column(Integer)


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

class Fact(Base):
    __tablename__ = 'fact_newest'

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
sql_engine = create_engine("mssql+pymssql://admin:Welcome@1@10.0.0.143/edgar_db_1")

psql_engine = create_engine("postgresql://xuspwr164:tOmh4nYXSGBFA4@public.xbrl.us:5432/edgar_db")

metadata.bind = sql_engine
for table in metadata.sorted_tables:
    table.create(sql_engine, checkfirst=True)

Session = sessionmaker(bind=sql_engine)

with open(f"error_log.txt", "a") as error_log:
    
    with psql_engine.connect() as psql_conn:
    
        with sql_engine.connect() as sql_conn:

            missing_reports = [325233, 308151, 301649, 301648, 301647, 301646, 301645, 301644, 301643, 301642, 301641, 301640, 301639, 301638, 301637, 301636, 301635, 301634, 301633, 301632, 301631, 301630, 301629, 301628, 301627, 301626, 301625, 301624, 301623, 301622, 301621, 301620, 301619, 301618, 301617]
       
            session = Session()

            for missing_report in missing_reports:
                try: 
                    sql_conn.execute(f"""DELETE FROM [dbo].[context_dimension_explicit_new] WHERE context_id IN (SELECT context_id FROM [dbo].[context_new] WHERE accession_id = {missing_report});
                                         DELETE FROM [dbo].[context_new] WHERE accession_id = {missing_report};
                                         DELETE FROM [dbo].[fact_newest] WHERE accession_id = {missing_report};""")

                    f_rows = psql_conn.execute(f'SELECT fact_id, accession_id, context_id, unit_id, unit_base_id, element_id, fact_value, xml_id, precision_value, decimals_value, is_precision_infinity, is_decimals_infinity, ultimus_index, calendar_ultimus_index, fiscal_ultimus_index, uom, is_extended, fiscal_year, fiscal_period, calendar_year, calendar_period, tuple_fact_id, fact_hash, calendar_hash, fiscal_hash, entity_id, element_namespace, element_local_name, dimension_count, inline_display_value, inline_scale, inline_negated, inline_is_hidden, inline_format_qname_id FROM public.fact WHERE accession_id = {missing_report};')
                    for f_row in f_rows:
                        session.add(Fact(**f_row))

                    c_rows = psql_conn.execute(f'SELECT * FROM public.context WHERE accession_id = {missing_report};')
                    for c_row in c_rows:              
                        session.add(Context(**c_row))
                        cde_rows = psql_conn.execute(f'SELECT * FROM public.context_dimension_explicit WHERE context_id = {c_row.context_id};')
                        for cde_row in cde_rows:              
                            session.add(ContextDimensionExplicit(**cde_row))
                    try:
                        session.commit()
                    except Exception as e:
                        error_log.write(f"report_id = {missing_report} Error: {e}")
                        session.rollback()

                except Exception as e:
                    error_log.write(f"report_id = {missing_report} Error: {e}")
                
                print(f'missing entries for report id {missing_report} downloaded')

            
            session.close()
        
            sql_conn.close()

        psql_conn.close()
        
    error_log.close()