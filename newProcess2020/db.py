import pandas as pd
from sqlalchemy import create_engine
def businessEachYear(year,engine):
	query = """
	SELECT *
	FROM strategy.companies
	WHERE country = "United States" AND (YEAR(founded_on) = %s
	OR YEAR(public_at) <= %s) LIMIT 10
	"""%(year,year)
	return pd.read_sql(query, con=engine)
