# 빌드 중 내려받지 않는다. README의 고정 버전을 직접 준비한다.
find_package(Threads REQUIRED)
if(NOT TARGET CppNetworkAsio)
    set(CPP_NETWORK_ASIO_ROOT "" CACHE PATH "standalone Asio 설치 또는 소스 루트")
    find_path(CPP_NETWORK_ASIO_INCLUDE_DIR asio.hpp
        HINTS "${CPP_NETWORK_ASIO_ROOT}" "${CMAKE_CURRENT_LIST_DIR}/../third-party/asio"
        PATH_SUFFIXES include asio/include NO_DEFAULT_PATH)
    if(NOT CPP_NETWORK_ASIO_INCLUDE_DIR)
        message(FATAL_ERROR "Asio 1.38.2 required. See README; set CPP_NETWORK_ASIO_ROOT or CPP_NETWORK_ASIO_INCLUDE_DIR.")
    endif()
    file(STRINGS "${CPP_NETWORK_ASIO_INCLUDE_DIR}/asio/version.hpp" asio_version_line
        REGEX "^#define ASIO_VERSION [0-9]+")
    if(NOT asio_version_line MATCHES "^#define ASIO_VERSION 103802([^0-9]|$)")
        message(FATAL_ERROR "This baseline requires Asio 1.38.2; found: ${asio_version_line}")
    endif()
    add_library(CppNetworkAsio INTERFACE IMPORTED)
    set_target_properties(CppNetworkAsio PROPERTIES
        INTERFACE_INCLUDE_DIRECTORIES "${CPP_NETWORK_ASIO_INCLUDE_DIR}"
        INTERFACE_COMPILE_DEFINITIONS ASIO_STANDALONE)
endif()
