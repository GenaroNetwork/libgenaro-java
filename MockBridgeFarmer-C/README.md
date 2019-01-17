# MockBridgeFarmer-C

A mock bridge and farmer for testing [libgenaro-java](https://github.com/GenaroNetwork/libgenaro-java) and [libgearo-android](https://github.com/GenaroNetwork/libgenaro-android).

## install dependencies

```shell
sudo apt-get install build-essential libjson-c-dev nettle-dev libmicrohttpd-dev libcurl4-gnutls-dev libuv1-dev cmake
git clone https://github.com/bitcoin-core/secp256k1.git /tmp/secp256k1
cd /tmp/secp256k1
./autogen.sh
./configure
make
sudo make install
```

## build

```shell
mkdir build
cd build
cmake ..
make
```

## run

```shell
cd build
./MockBridgeFarmer
```
