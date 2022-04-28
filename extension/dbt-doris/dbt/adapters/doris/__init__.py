from dbt.adapters.base import AdapterPlugin
from dbt.adapters.doris.connections import DorisAdapterCredentials
from dbt.adapters.doris.impl import DorisAdapter
from dbt.include import doris

Plugin = AdapterPlugin(
    adapter=DorisAdapter,
    credentials=DorisAdapterCredentials,
    include_path=doris.PACKAGE_PATH,
)
