# 검증 기록

## 2026-11-21 송신 완료·정상 종료

`feature/drain-close`에서 로컬 쓰기 완료 통지와 기한이 있는 close_after_flush를 검사했다. macOS Debug·Release·ASan+UBSan·TSan에서 각각 CTest 31개를 통과했다. 설치 소비자 2개도 통과했으며 새 비동기 예제는 완료 바이트 수와 정상 종료 API를 함께 검사한다.

송신 수락과 완료의 태그·순서·개수 일치, 미시작·진행 중 취소, 부분 쓰기, 정상 종료와 EOF, 빈 큐, 기한 초과, drain 중 파괴, 콜백 내 재송신, 콜백 예외, 쓰기 오류, 다중 producer의 송신·drain·즉시 종료 경쟁을 추가했다. 공개 원격 작업은 사용자 요청으로 보류했으며 이 기능의 Linux·Windows 검증은 아직 수행하지 않았다.

## 2026-11-21 연결 수락·접속

`feature/accept-connect`의 Listener·Connector, 비동기 왕복 예제와 설치 소비자를 검사했다.

| 구성 | 결과 |
| --- | --- |
| macOS Apple Clang 21 / Debug | CTest 21개 통과 |
| macOS Apple Clang 21 / Release | CTest 21개 통과 |
| macOS ASan + UBSan | CTest 21개 통과, UBSAN_OPTIONS=halt_on_error=1 |
| macOS TSan | CTest 21개 통과 |
| 설치 패키지 사용 프로젝트 | 독립 구성·빌드·실제 소켓 왕복 2개 통과 |
| 원격 macOS·Linux·Windows | 사용자 요청으로 공개 게시·CI 보류 |

새 검사는 접속 성공과 소유권 이전, 거절 후 새 시도, 기한을 지난 접속과 지연된 완료 처리, 시작 전후 취소, 수락 일시정지와 콜백 안 재개, 대기 accept 종료, 객체 파괴, 콜백 예외, 다중 스레드 중복 요청을 다룬다. 성능·전체 연결 수용량 검증은 후속 단계다.

초기 커밋 `75f8f7d`는 [독립 CI](https://github.com/lemondory/cpp-network/actions/runs/44767756982)의 macOS·Ubuntu·Windows Debug/Release와 Linux ASan·TSan, 8개 구성에서 모두 통과했다. 아래 최초 기록의 원격 미실행 상태는 이 결과로 해소됐다.

## 2026-10-11 최초 로컬 검증

2026-10-11, 로컬 미커밋 작업 트리. 실제 서비스 성능·수용 연결 수·운영 준비 상태는 측정하지 않았다.

| 구성 | 결과 |
| --- | --- |
| macOS Apple Clang 21 / Debug | CTest 10개 통과 |
| macOS Apple Clang 21 / Release | CTest 10개 통과 |
| macOS ASan + UBSan | CTest 10개 통과, UBSAN_OPTIONS=halt_on_error=1 |
| macOS TSan | CTest 10개 통과 |
| 설치 패키지 사용 프로젝트 | 독립 구성·빌드·실제 소켓 왕복 1개 통과 |
| 다른 Asio 버전 지정 | CMake 구성 단계에서 거절 확인 |
| Linux Ubuntu 24.04 컨테이너 | 패키지 서버 접속 시간 초과로 컴파일러 준비 실패, 코드 빌드·검사 미실행 |
| Windows | 미실행, 원격 자동 검사 필요 |

검사 범위:

- 바이트·메시지 수 상한 각각의 거절과 무소비, 빈 입력·과대 입력·잘못된 제한.
- 호출자 원본 변경 이후 복사본 전달, 작은 수신 청크의 바이트 순서와 공간 반환 통지.
- 수신 중단 후 추가 전달 없음, 재개 후 다시 중단.
- 송신자 6개가 항목 1,800개를 요청하는 동안 실행 스레드 4개로 수신: 유실·중복·바이트 섞임·송신자별 순서·수신 콜백 동시 실행 검사.
- 여러 송신자의 종료 요청 경쟁, 종료 후 송신 거절과 예약량 해제.
- 읽지 않는 상대에게 큰 송신이 진행 중일 때 상한 유지와 취소 완료.
- 상대 종료, 사용자 수신 함수 예외, 읽기 중 공개 객체 파괴, 종료 통지 한 번.
- 한 프로세스 안의 두 실제 TCP 소켓 왕복 예제.

최초 macOS 테스트 실행은 샌드박스의 로컬 bind 제한으로 실패했다. 로컬 소켓 권한으로 재실행한 결과가 표의 값이다. 설치 패키지 소비자 구성에서는 Command Line Tools와 Xcode의 서로 다른 SDK가 선택되어 링크가 실패했고, macOS용 `CMAKE_OSX_SYSROOT=macosx`를 지정해 실제 구성·빌드·검사를 완료했다.

Linux 컨테이너는 소스를 읽기 전용으로 연결했다. Ubuntu 패키지 서버의 HTTP·HTTPS 접속 모두 시간 초과였으며 검사 코드를 실행한 결과가 아니다. 임시 컨테이너는 `--rm`으로 제거했다. 인증서 검증을 끄지 않았다.

`.github/workflows/build.yml`은 3개 운영체제의 Debug/Release, Linux sanitizer, 설치 소비자 검사를 정의한다. 아직 이 저장소에서 원격 실행한 결과는 없다. 첫 커밋·푸시 후 확인해야 한다.

<!-- ASan(AddressSanitizer): 메모리 오류 검사. UBSan(UndefinedBehaviorSanitizer): 정의되지 않은 동작 검사. -->
<!-- TSan(ThreadSanitizer): 스레드 간 데이터 경합 검사. TCP(Transmission Control Protocol): 신뢰성 바이트 스트림 전송 규약. -->
<!-- SDK(Software Development Kit): 플랫폼용 헤더·라이브러리 등의 개발 도구 묶음. -->
<!-- HTTP(Hypertext Transfer Protocol), HTTPS(Hypertext Transfer Protocol Secure): 웹 전송 규약과 암호화된 웹 전송 규약. -->
