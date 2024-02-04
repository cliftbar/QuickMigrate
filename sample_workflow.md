# Sample Workflow
1. Set up db_configs.yaml from the template file
    - Currently, the tool only supports Postgres
    - Set up default first
    - `connection_warning` causes the tool to give a warning about what DB is being connected
2. Run Database `setup` task
3. Create some revisions
    - either start with the template file or use the `new_revision` task
    - Sample revisions are included
4. Check dependencies with `show_all_layers` task
5. Apply revisions using the `apply_each` task.  The tool will ask for confirmation at each revision
