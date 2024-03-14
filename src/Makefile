start:
	docker compose build
	docker compose up --detach
	docker compose kill --signal SIGKILL Dummy-Server
	docker compose rm Dummy-Server --force --stop
	docker compose logs --follow

stop:
	-docker compose kill --signal SIGTERM Load-Balancer
	-docker compose wait Load-Balancer
	docker compose down --remove-orphans

