EXTENSION = clustered_pg
MODULES = clustered_pg
DATA = sql/clustered_pg--0.1.0.sql
DOCS =
REGRESS = clustered_pg

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
