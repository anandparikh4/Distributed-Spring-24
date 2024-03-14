start:
	docker compose build
	docker compose up --detach
	docker compose kill --signal=SIGKILL Dummy-Server
	docker compose rm Dummy-Server --force --stop
	docker compose logs --follow

stop:
	docker compose stop Load-Balancer --timeout 100
	docker compose down --remove-orphans

