"""SQL templates for population processing.

All DuckDB SQL queries are centralized here for maintainability.
Templates use string formatting with named placeholders.

Submodules are grouped by purpose; wildcard re-exports preserve
the flat ``sql.TEMPLATE_NAME`` access pattern for all consumers.
"""

from passculture.data.insee_population.sql.base import *  # noqa: F403
from passculture.data.insee_population.sql.mobility import *  # noqa: F403
from passculture.data.insee_population.sql.projections import *  # noqa: F403
from passculture.data.insee_population.sql.ratios import *  # noqa: F403
from passculture.data.insee_population.sql.stats import *  # noqa: F403
