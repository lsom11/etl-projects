.PHONY: create-docker-environment
create-docker-environment:
	@chmod +x start.sh
	@sudo docker-compose -f docker/docker-compose.py3.yml up -d --build --force-recreate --remove-orphans
	@sudo docker image prune -f

.PHONY: restart-docker-environment
restart-docker-environment:
	@sudo docker-compose -f docker/docker-compose.py3.yml up -d --build

.PHONY: kill-docker-environment
kill-docker-environment:
	@sudo docker-compose -f docker/docker-compose.py3.yml down

.PHONY: environment
environment:
	@pyenv install -s 3.8.1
	@pyenv virtualenv 3.8.1 etl-projects
	@pyenv local etl-projects

.PHONY: requirements
requirements:
	@python -m pip install -U -r requirements.txt

.PHONY: check-style
## check style with flake8 and black
check-style:
	@echo ""
	@echo "Check Style"
	@echo "=========="
	@echo ""
	@python -m black etl_projects/
	@python -m flake8 --config=setup3.cfg etl_projects/