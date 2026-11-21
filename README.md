# cpp-network

C++20과 **standalone Asio**로 구현하는 독립 네트워크 라이브러리입니다. 게임 메시지·인증·데이터베이스에 의존하지 않습니다.

`Listener`의 비동기 연결 수락과 `Connector`의 기한·취소가 있는 접속 시도로 TCP 소켓을 준비하고, `Connection`으로 바이트를 송수신합니다. 송신 대기량 제한, 수신 중단·재개, 콜백 직렬화, 요청별 로컬 쓰기 완료와 기한이 있는 정상 종료를 제공합니다. 전체 연결 관리는 후속 범위입니다.

## 구조

```text
include/cpp_network/  공개 헤더
src/                 연결 구현
examples/            실제 로컬 소켓 왕복 예제
tests/               경계·동시성 검사와 설치 패키지 사용 예제
cmake/               의존성 탐색·패키지 내보내기
docs/                실행·수명 계약, Git 추적
third-party/asio/    수동으로 준비한 외부 의존성, Git 제외
out/                 빌드·설치 결과, Git 제외
```

## 의존성 수동 준비

CMake 3.25 이상과 C++20 컴파일러가 필요합니다. Asio는 **1.38.2**, 공식 소스 커밋 **`8806a6803cde7054c3049d3666d3ec36786568c5`**로 기준을 고정합니다. Boost는 설치하지 않습니다. 구성·빌드는 자동 다운로드를 하지 않습니다.

저장소 루트에서 한 번 실행합니다.

```sh
git clone --depth 1 --branch asio-1-38-2 https://github.com/chriskohlhoff/asio.git third-party/asio
git -C third-party/asio rev-parse HEAD
```

출력이 위 커밋과 같은지 확인합니다. 기존 디렉터리에 덮어쓰거나 로컬 변경을 초기화하지 마세요. 이 배포본의 헤더 위치는 `third-party/asio/include`입니다. 다른 설치 위치를 사용하려면 `-DCPP_NETWORK_ASIO_ROOT=/설치/경로` 또는 `-DCPP_NETWORK_ASIO_INCLUDE_DIR=/asio.hpp가/있는/경로`를 지정합니다. 다른 버전은 구성 단계에서 거절합니다. [Asio standalone 안내](https://think-async.com/Asio/AsioStandalone.html)

## 빌드·검사

```sh
cmake --preset macos-debug
cmake --build --preset macos-debug --parallel 4
ctest --preset macos-debug
./out/build/macos-debug/tcp-roundtrip
./out/build/macos-debug/async-roundtrip
```

Linux는 `linux-debug`/`linux-release`, Windows는 `windows-msvc` 구성 후 `windows-debug`/`windows-release`로 빌드·검사합니다. macOS의 `macos-asan`, `macos-tsan` 프리셋은 메모리 오류·데이터 경합 검사용입니다. 다른 Clang/GCC 환경에서도 `CPP_NETWORK_SANITIZER=address` 또는 `thread`를 지정할 수 있습니다.

`tcp-roundtrip`은 이미 연결된 소켓의 왕복 바이트를 확인합니다. `async-roundtrip`은 Listener와 Connector로 연결을 준비한 뒤 Connection으로 왕복합니다. 모두 실제 localhost 소켓을 사용합니다. 여러 연결의 등록·상한·종료를 관리하는 장기 실행 서버는 다음 범위입니다.

## 사용하는 프로젝트에 연결

소스로 함께 빌드하려면 `add_subdirectory`로 이 저장소를 추가하고 `cpp_network::transport`를 링크합니다. 예제와 검사는 하위 프로젝트일 때 기본으로 꺼집니다. 상위 게임 폴더의 이름이나 경로에 의존하지 않습니다.

설치 패키지를 사용하는 경우:

```sh
cmake --install out/build/macos-debug --prefix "$PWD/out/install"
cmake -S tests/consumer -B out/consumer -DCMAKE_OSX_SYSROOT=macosx -DCMAKE_PREFIX_PATH="$PWD/out/install" -DCPP_NETWORK_ASIO_ROOT="$PWD/third-party/asio"
cmake --build out/consumer
ctest --test-dir out/consumer --output-on-failure --no-tests=error
```

위 예시는 macOS용이며 Linux·Windows에서는 `-DCMAKE_OSX_SYSROOT=macosx`를 제외합니다.

사용 프로젝트에서는 `find_package(CppNetwork 0.1.0 EXACT CONFIG REQUIRED)`와 `target_link_libraries(대상 PRIVATE cpp_network::transport)`를 사용합니다. 패키지에는 Asio를 복사해서 설치하지 않습니다. 공개 헤더에 Asio socket 타입이 있으므로 사용하는 환경에도 동일 버전의 Asio가 필요합니다. Windows 설치 시 `--config Debug` 또는 `Release`를 빌드 구성과 맞춥니다.

라이브러리 자체 코드와 설치 안내는 이 저장소에 커밋합니다. `third-party/`, `out/`, 기기별 `CMakeUserPresets.json`은 제외합니다. Asio는 자체 라이선스를 가진 외부 의존성이므로 그 배포본의 라이선스를 유지합니다.

상세 동작은 [연결 송수신 계약](docs/01-connection-contract.md), [연결 수락·접속 계약](docs/03-establishment-contract.md), [송신 완료·정상 종료 계약](docs/05-drain-contract.md)을 참고합니다. [네트워크 작업 순서](docs/04-network-plan.md)에 단계별 완료 기준과 브랜치를 기록합니다. 게임 통합·DB 드라이버·전투 메시지는 포함하지 않습니다.

<!-- TCP(Transmission Control Protocol): 순서 있는 신뢰성 바이트 스트림 전송 규약. -->
<!-- DB(Database): 데이터베이스. GCC(GNU Compiler Collection): 컴파일러 도구 모음. -->
<!-- ASan(AddressSanitizer): 메모리 오류 검사. TSan(ThreadSanitizer): 스레드 간 데이터 경합 검사. -->
<!-- callback: 비동기 결과를 받는 함수. standalone Asio: Boost 없이 사용하는 Asio 구성. -->
