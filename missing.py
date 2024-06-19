from sqlalchemy import create_engine

sql_engine = create_engine("mssql+pymssql://admin:Welcome@1@./edgar_db")

with sql_engine.connect() as sql_conn:

    rep_count = 100
    missing_reports = []
    i = 0
    # count = sql_conn.execute(f'SELECT count(1) as count FROM dbo.report;')
    # for row in count:
    #     rep_count = row['count']

    while i < rep_count:
        reports = []
        try:
            r_rows = sql_conn.execute(f'SELECT report_id FROM dbo.report ORDER BY report_id DESC OFFSET {i} ROWS FETCH NEXT 50 ROWS ONLY;')
            for r_row in r_rows:
                reports.append(r_row.report_id) 
        except:
            continue
        else:
            for report in reports:
                f_row_counts = sql_conn.execute(f'SELECT count(1) as count FROM dbo.fact_newest WHERE accession_id = {report};')
                for f_row_count in f_row_counts:
                    if f_row_count['count'] == 0: missing_reports.append(report)

                c_row_counts = sql_conn.execute(f'SELECT count(1) as count FROM dbo.context_new WHERE accession_id = {report};')
                for c_row_count in c_row_counts:
                    if c_row_count['count'] == 0: missing_reports.append(report)

                
            i += 50

    sql_conn.close()

print(f"reports missing facts: {missing_fact}\n")
print(f"reports missing contexts: {missing_context}\n")