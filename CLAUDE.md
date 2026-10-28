# CLAUDE.md

Netty 4.1 기반 Redis 호환 인메모리 캐시 서버(Java 21, Maven). 학습과 실험이 목적이며, Redis가 구조적으로 불리한 상황에서 JVM으로 더 나은 결과를 낼 수 있는지 검증한다.

## 작업 전에

- **`intent.md`를 먼저 읽는다.** 모든 변경은 그 문서의 의도, 비목표, 판단 규칙에 맞아야 한다.
- 변경이 비목표에 해당하거나 판단 규칙과 충돌하면 구현하지 말고 먼저 사용자에게 알린다.
- 방향 자체가 바뀌는 결정이 나오면 `intent.md` 갱신을 제안한다.

## 구조

- `src/main/java/io/netty/example/server/RedisServer.java`: 서버 진입점. 포트는 `-Dport` (기본 6379)
- `src/main/java/io/netty/example/handler/RedisServerHandler.java`: 명령 처리 (GET, SET, DEL, COMMAND, SHUTDOWN)
- `src/test/java/.../RedisServerHandlerTest.java`: `EmbeddedChannel`로 전체 파이프라인을 거치는 단위 테스트
- `.claude/skills/verify-redis-server/`: 실제 TCP로 서버를 검증하는 스킬
- `compose.yaml`, `Dockerfile`: 실제 Redis와 나란히 띄워 `redis-benchmark`로 비교하는 환경

## 명령

```bash
mvn -B test                                    # 단위 테스트
docker compose up -d --build --wait            # Redis(6379) + netty-redis(6380)
docker compose run --rm benchmark -h netty-redis -t set,get -n 100000 -c 50 -q
docker compose down
```

worker 스레드 수와 JVM 옵션은 `NETTY_JAVA_OPTS`로 바꾼다 (예: `-Dio.netty.eventLoopThreads=1`).

## 규칙

- 한 요청에는 정확히 한 응답을 보내고, 잘못된 요청에도 연결을 끊지 않는다.
- 동작을 바꾸면 단위 테스트를 추가하고, 서버 동작에 대한 주장은 `verify-redis-server` 스킬로 확인한다.
- 성능 주장은 `compose.yaml` 환경에서 실제 Redis와 함께 측정한 결과로만 한다.
- 코드가 이미 말하는 내용을 반복하는 주석은 쓰지 않는다.
- 작업은 GitHub 이슈를 먼저 만들고, 브랜치는 `<type>/<issue>-<설명>`, 커밋은 `<type>: <한국어 설명> (#이슈)` 형식을 따른다.
- 설계 문서, 분석, 실험 결과는 저장소가 아니라 Notion(`intent.md`의 관련 문서 위치)에 작성한다.
