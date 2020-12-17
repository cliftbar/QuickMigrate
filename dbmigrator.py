import urllib.parse

import yaml
import sqlalchemy

from enum import Enum
from pathlib import Path
from typing import Dict, Optional, Union, List, Any, Set

from sqlalchemy import MetaData, Table, Column, Text
from sqlalchemy.engine.base import Engine, Connection
from sqlalchemy.engine.result import ResultProxy
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.orm import Query
from sqlalchemy.sql import Insert, Select


class DBConfiguration:
    def __init__(self,
                 dialect: str,
                 host: Optional[str],
                 port: Optional[int],
                 username: Optional[str],
                 password: Optional[str],
                 database: Optional[str],
                 connection_warning: bool = False):
        self.dialect: str = dialect
        self.host: Optional[str] = host
        self.port: Optional[int] = port
        self.username: Optional[str] = username
        self.password: Optional[str] = password
        self.database: Optional[str] = database
        self.connection_warning: bool = connection_warning

    def connection_string(self) -> str:
        """
        Generate a SQL connection string from the configuration suitable for SQLAlchemy
        :return: SQL Connection String
        """
        if self.dialect == "sqlite":
            ret_connection_string = f"{self.dialect}:///{self.database}"
        else:
            escaped_password: str = urllib.parse.quote_plus(self.password)
            auth_section: str = f"{self.username}:{escaped_password}"
            address: str = f"{self.host}:{self.port}"
            ret_connection_string = f"{self.dialect}://{auth_section}@{address}/{self.database}"

        return ret_connection_string

    @staticmethod
    def from_yaml(input_yaml: Dict) -> "DBConfiguration":
        """
        Create a DBConfiguration object from a yaml load Dict
        :param input_yaml: Dict loaded from yaml
        :return: DBConfiguration object
        """
        return DBConfiguration(input_yaml["dialect"],
                               input_yaml["host"],
                               input_yaml["port"],
                               input_yaml["username"],
                               input_yaml["password"],
                               input_yaml["database"],
                               input_yaml.get("connection_warning", False))


class DBRevision:
    def __init__(self,
                 revision_name: str,
                 dependencies: Set[str],
                 sql_text: Optional[str],
                 active: bool,
                 description: Optional[str] = None):
        self.revision_name: str = revision_name
        self.dependencies: Set[str] = dependencies
        self.sql_text: Optional[str] = sql_text
        self.active: bool = active
        self.description: Optional[str] = description

    @staticmethod
    def from_yaml(input_yaml: Dict) -> "DBRevision":
        """
        Create a DBRevision object from a yaml load Dict
        :param input_yaml: Dict loaded from yaml
        :return: DBRevision object
        """
        return DBRevision(input_yaml["revision_name"],
                          set(input_yaml.get("dependencies")),
                          input_yaml["sql_text"],
                          input_yaml["active"],
                          input_yaml.get("description"))

    def to_yaml(self) -> Dict:
        """
        Serialize the DBRevision object into a Dict suitable for dumping into a yaml file 
        :return: yaml suitable Dict
        """
        return {
            "revision_name": self.revision_name,
            "dependencies": list(self.dependencies),
            "sql_text": self.sql_text,
            "active": self.active,
            "description": self.description
        }

    @staticmethod
    def yaml_template(revision_name: str, dependencies: Set[str]) -> "DBRevision":
        """
        Create a DBRevision template with the given parameters
        :param revision_name: What to name the revision
        :param dependencies: Dependency revisions
        :return: DBRevision Object
        """
        return DBRevision(revision_name,
                          dependencies,
                          None,
                          True,
                          None)


class DBMigrator:
    class _MigrationTableColumns(Enum):
        revisions = "revisions"

    def __init__(self,
                 db_config: DBConfiguration,
                 migrations: Path,
                 migration_table_schema: str = "public",
                 revisions_table: str = "revisions"):
        self.db_config: DBConfiguration = db_config
        self.migrations: Path = migrations

        self.psql_engine: Engine = sqlalchemy.create_engine(self.db_config.connection_string())
        self.sql_metadata: MetaData = MetaData(bind=self.psql_engine)

        self.revision_table_schema: str = migration_table_schema
        self.revision_table: str = revisions_table

        self.applied_revisions: Set[str] = self.get_applied_revisions()

    def load_revisions(self) -> Dict[str, DBRevision]:
        """
        Load the revisions from yml files
        :return: Dictionary of revision name and revision
        """
        ret_revisions: Dict[str, DBRevision] = {}
        for revision_path in self.migrations.rglob("*.y?ml"):
            revision_dict: Dict = yaml.load(revision_path.open(), Loader=yaml.SafeLoader)
            revision: DBRevision = DBRevision.from_yaml(revision_dict)
            if revision.active:
                ret_revisions[revision.revision_name] = revision

        return ret_revisions

    def db_setup(self):
        """
        Creates the migration tracking table for dbmigrator
        :return:
        """
        revision: Table = Table(self.revision_table,
                                self.sql_metadata,
                                Column(self._MigrationTableColumns.revisions.value, Text, primary_key=True),
                                schema=self.revision_table_schema)
        revision.create(self.psql_engine)

    def _get_table_reflection(self, schema: str, table: str) -> Table:
        """
        Get the SQLAlchemy object for a table from metadata, the starting point for most sqlalchemy queries
        :param schema: DB Schema
        :param table: DB Table name
        :return: SQLAlchemy Table object
        """
        return self.sql_metadata.tables.get(f"{schema}.{table}",
                                            Table(table, self.sql_metadata, schema=schema, autoload=True))

    def _execute_query(self,
                       sql_connection: Connection,
                       sql_query: Union[str, Query]) -> List[Dict[str, Any]]:
        """
        Execute a DB query using the connection, return the results as a List (rather than as a generator)
        :param sql_connection: DB connection to use
        :param sql_query: SQL query to run, can be a raw string or SQLAlchemy query
        :return: results as a List of Dicts, if there are returned rows
        """
        return_result: List[Dict[str, Any]] = []
        result: ResultProxy = sql_connection.execute(sql_query)
        if result and result.returns_rows:
            return_result: List[Dict[str, Any]] = [dict(row) for row in result]
        return return_result

    def get_applied_revisions(self) -> Set[str]:
        """
        Reads the revisions applied in the DB from the migrators table.  Returns an empty set if the table doesn't exist
        :return: applied revisions as a set
        """
        try:
            with self.psql_engine.connect() as sql_conn:

                migration_table: Table = self._get_table_reflection(self.revision_table_schema, self.revision_table)

                revision_select: Select = migration_table.select()
                revision_results: List[Dict[str, Any]] = self._execute_query(sql_conn, revision_select)
        except NoSuchTableError:
            revision_results: List[Dict[str, Any]] = []
        applied_revisions: Set[str] = set([row[self._MigrationTableColumns.revisions.value]
                                           for
                                           row in revision_results])

        return applied_revisions

    def create_new_revision(self,
                            revision_name: str,
                            dependencies: Optional[Set[str]] = None,
                            description: Optional[str] = None) -> DBRevision:
        """
        Creates a new revision file with basic information in the revisions directory, returns the revision object
        :param revision_name: Name of the revision
        :param dependencies: Names of revisions the new revision depends on (Optional)
        :param description: description text for the new revision (Optional)
        :return: New revision object
        """
        if dependencies is None:
            dependencies = []
        revision_template = DBRevision.yaml_template(revision_name, dependencies)
        if description is not None:
            revision_template.description = description

        revision_path: Path = self.migrations / Path(f"{revision_template.revision_name}.yaml")

        with revision_path.open("w") as revision_file:
            yaml.dump(revision_template.to_yaml(), revision_file)

        return revision_template

    def build_revision_layers(self, require_all: bool = True, include_applied: bool = False) -> List[Set[str]]:
        """
        Builds the dependency layers for revisions.  Each layer is the set of revisions that can be applied where each
            revisions dependencies are met in previous layers. Meaning all revisions in Layer 0 have no dependencies,
            all revisions in Layer 1 have their dependencies met by Layer 0, etc
        :param require_all: Require that all dependencies are met for all revisions.  If a revision has an unmet
            dependency and this flag is True, an exception will be raised
        :param include_applied: Whether to include already applied revisions in the layer building process.  If
            included, applied revisions are in Layer 0
        :return: Dependency Layers
        """
        revisions: Dict[str, DBRevision] = self.load_revisions()

        revision_layers: List[Set[str]] = []
        flat_revision_layers: Set[str] = set()
        if include_applied:
            revision_layers.append(self.applied_revisions)
        flat_revision_layers: Set[str] = flat_revision_layers.union(self.applied_revisions)
        for rev in self.applied_revisions:
            revisions.pop(rev, None)

        while True:
            new_layer: Set[str] = set()
            revision_names: List[str] = list(revisions.keys())
            for rev_name in revision_names:
                if (revisions[rev_name].dependencies.issubset(flat_revision_layers)
                        and rev_name not in flat_revision_layers):
                    new_layer.add(rev_name)
                    revisions.pop(rev_name)

            # Stop building layers when either no new revisions were put into the layer,
            #   or all revisions have been accounted for
            if len(new_layer) == 0:
                break
            else:
                flat_revision_layers = flat_revision_layers.union(new_layer)
                revision_layers.append(new_layer)

            if len(revisions.keys()) == 0:
                break

        if require_all and len(revisions) != 0:
            raise Exception(f"could not resolve dependencies for the following revisions: {revisions.keys()}")

        return revision_layers

    def apply_revision(self, revision: DBRevision):
        """
        Apply a revision to the DB
        :param revision: DBRevision object
        :return:
        """
        if revision.dependencies:
            if not revision.dependencies.issubset(self.applied_revisions):
                raise Exception(f"Dependencies for revision {revision.revision_name} not met: {revision.dependencies}")

        migration_table: Table = self._get_table_reflection(self.revision_table_schema, self.revision_table)
        with self.psql_engine.connect() as sql_conn:
            with sql_conn.begin():
                self._execute_query(sql_conn, revision.sql_text)
                revision_insert: Insert = migration_table.insert(
                    {self._MigrationTableColumns.revisions.value: revision.revision_name}
                )
                self._execute_query(sql_conn, revision_insert)
                self.applied_revisions.add(revision.revision_name)

    def get_revision_dependencies(self, revision_name: str) -> List[Set[str]]:
        """
        Build a set of dependency layers for a given revision.
        :param revision_name:
        :return: Dependency Layers
        """
        revisions: Dict[str, DBRevision] = self.load_revisions()
        revision_layers: List[Set[str]] = [{revision_name}]

        while True:
            new_layer: Set[str] = set()
            for rev in revision_layers[-1]:
                new_layer = new_layer.union(revisions[rev].dependencies)

            if len(new_layer) == 0:
                break

            revision_layers.append(new_layer)
        revision_layers.reverse()
        return revision_layers
