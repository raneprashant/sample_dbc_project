# sample_dbc_project, steps that we followed and studied to setup

Folder
→ Git initialize
→ GitHub remote + first push
→ Install Databricks VS Code extension
→ Install Databricks CLI
→ Configure/authenticate Databricks CLI profile(s)
→ Configure this repository in Databricks VS Code extension
     → choose DEV workspace/profile
     → select Serverless or required compute
→ Create .venv
→ Select .venv as VS Code Python interpreter
→ Install compatible databricks-connect in .venv
→ Enable/test Databricks Connect
→ Test Unity Catalog / real table access
→ Configure Bundle (databricks.yml)
→ Define Job/Pipeline resources
→ bundle validate / plan / deploy / run
→ Git commit / push


# sample_dbc_project, steps that we will follow for development and deployment through CI/CD 

1) Developer makes changes in VS Code on a feature branch.
2) Commit locally and push the feature branch to GitHub.
3) Create PR: feature → develop.
4) After PR approval/merge, GitHub Actions deploys develop → Databricks dev target.
5) Run/test the job in dev.
6) Promote through PR into the release branch.
7) After approval/merge, GitHub Actions deploys release → Databricks test target.
8) Run/test the job in test.
9) Create PR: release → main.
10) After approval/merge, GitHub Actions deploys main → Databricks prod target.