# Basic Flink Usage

## StringWordCountJob

To run the StringWordCountJob, you need to use `nc`(NetCat) to send data to the Flink cluster.

How to install NetCat:

```bash
yum install nc                  [On CentOS/RHEL]
dnf install nc                  [On Fedora 22+ and RHEL 8]
sudo apt-get install Netcat     [On Debian/Ubuntu]
```

How to use NetCat:

```bash
nc -lk 9999
```

Send data to the Flink cluster:
```bash
~ nc -lk 9999
liu,liu,liu,xiang,xiang
liu,liu,liu,xiang,xiang
liu,liu,liu,xiang,xiang
```