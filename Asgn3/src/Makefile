start:
	docker compose up --build --detach
	docker compose kill --signal SIGKILL Dummy-Server
	docker compose rm Dummy-Server --force --stop
	docker compose logs --follow

stop:
	-docker compose kill --signal SIGTERM Load-Balancer Shard-Manager Database
	-docker compose wait Load-Balancer Shard-Manager Database
	docker compose down --remove-orphans

