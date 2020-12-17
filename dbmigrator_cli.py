import argparse
import yaml

from argparse import Namespace
from pathlib import Path
from typing import Dict, Optional, List, Set

from dbmigrator import DBConfiguration, DBMigrator, DBRevision


class DBMigratorCLI:
    @staticmethod
    def load_db_config(db_config: Path, db_config_name: str = "default") -> DBConfiguration:
        """
        Load the DB connection config file
        :param db_config: Path to config file
        :param db_config_name: Which connection to use in the config file
        :return:
        """
        with db_config.open("r") as db_file:
            configs: Dict = yaml.load(db_file, Loader=yaml.SafeLoader)
        return DBConfiguration.from_yaml(configs[db_config_name])

    def new_revision(self, migrator: DBMigrator):
        """
        Create a new revision file
        :param migrator: migrator class
        :return:
        """
        revision_name: str = input(f"Revision Name: ")
        description_input: str = input(f"Add a description? Y/[N]: ") or "N"
        description: Optional[str] = None
        if description_input.lower() == "y":
            description = input(f"Revision Description: ")
        migrator.create_new_revision(revision_name, description=description)

    def show_all_layers(self, migrator: DBMigrator):
        """
        Print all the revision dependency layers.  Applied revisions are in Layer 0.  An exception is thrown if there
            is a missing dependency in the revisions
        :param migrator: migrator class
        :return:
        """
        print(f"")
        revision_layers = migrator.build_revision_layers(include_applied=True)
        print(f"Revision Dependency Layers (Applied Revisions in Layer 0)")
        self.print_dependency_layers(revision_layers)

    def show_revision_layers(self, migrator: DBMigrator):
        """
        Print all the revision dependency layers.
        :param migrator: migrator class
        :return:
        """
        revision_name: str = input(f"Revision Name: ")
        revision_layers: List[Set[str]] = migrator.get_revision_dependencies(revision_name)
        print(f"Revision Dependency Layers (Includes Applied Revisions)")
        self.print_dependency_layers(revision_layers)

    def apply_all(self, migrator: DBMigrator):
        """
        Apply all the revisions to the DB
        :param migrator: migrator class
        :return:
        """
        revision_layers = migrator.build_revision_layers(include_applied=False)
        print(f"Revision Dependency Layers")
        self.print_dependency_layers(revision_layers)

        do_revisions: str = input(f"Apply revisions? Y/[N]: ")
        if do_revisions.lower() != "y":
            print(f"exiting")
            exit()

        self._apply_revision_layers(migrator, revision_layers, True)

    def apply_each(self, migrator: DBMigrator):
        """
        Apply all the revisions to the DB, asking for confirmation at each revision
        :param migrator: migrator class
        :return:
        """
        revision_layers = migrator.build_revision_layers(require_all=True, include_applied=False)
        print(f"Revision Dependency Layers")
        self.print_dependency_layers(revision_layers)

        self._apply_revision_layers(migrator, revision_layers)

    def _apply_revision_layers(self, migrator: DBMigrator, revision_layers: List[Set[str]], accept_all: bool = False):
        """
        Private method for applying all the layers
        :param migrator: migrator class
        :param revision_layers:
        :param accept_all: Whether check for confirmation for each revision
        :return:
        """
        revisions: Dict[str, DBRevision] = migrator.load_revisions()

        for layer in revision_layers:
            for rev_name in layer:
                rev: DBRevision = revisions[rev_name]
                print(f"applying revision {rev.revision_name} - {rev.description}")
                revision_confirm: Optional[str] = None
                if not accept_all:
                    revision_confirm = input("Continue? Y/[N]: ")
                if accept_all or revision_confirm.lower() == "y":
                    migrator.apply_revision(rev)

    def print_dependency_layers(self, revision_layers: List[Set[str]]):
        for i in range(len(revision_layers)):
            print(f"\tLayer {i}: {', '.join(revision_layers[i])}")
        print()

    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--config", "-c", default="db_configs.yaml", help="path to db connection configuration")
        parser.add_argument("--revisions", "-r", default="./revisions", help="path to db migrations")
        parser.add_argument("--db", "-d", default="default", help="which database config to use")
        parser.add_argument("task", default="help", help='Which action to run.  Use "help" for more task information.')

        args: Namespace = parser.parse_args()

        db_config_name: str = args.db
        db_configs: Path = Path(args.config)
        migrations: Path = Path(args.revisions)
        task: str = args.task

        db_connection_config: DBConfiguration = self.load_db_config(db_configs, db_config_name)
        if db_connection_config.connection_warning:
            warning_input: str = input(f"WARNING: Connection to database {db_connection_config.database} at "
                                       f"{db_connection_config.host}:{db_connection_config.port}, continue? Y/[N]: ")
            if not warning_input.lower() == "y":
                print("exiting")
                exit()

        migrator: DBMigrator = DBMigrator(db_connection_config, migrations)

        if task == "setup":
            migrator.db_setup()
        elif task == "new_revision":
            self.new_revision(migrator)
        elif task == "show_all_layers":
            self.show_all_layers(migrator)
        elif task == "apply_all":
            self.apply_all(migrator)
        elif task == "apply_each":
            self.apply_each(migrator)
        elif task == "show_revision_layers":
            self.show_revision_layers(migrator)
        else:
            print("\n".join(
                [f"task options:",
                 f"\tsetup: Initialize DB for migrations",
                 f"\tnew_revision: Create a new revision file with basic information",
                 f"\tshow_all_layers: Prints the dependency layers for all revisions",
                 f"\tshow_revision_layers: Prints the dependency layers for a given revision",
                 f"\tapply_all: Applies all revisions, asking for confirmation before starting",
                 f"\tapply_each: Applies each revision, asking for confirmation before each revision is run"]
            ))


if __name__ == "__main__":
    cli: DBMigratorCLI = DBMigratorCLI()
    cli.main()
