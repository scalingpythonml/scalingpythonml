#!/bin/bash
set -ex
if [ ! -f /updated_pi ]; then
  DEBIAN_FRONTEND=noninteractive
  export DEBIAN_FRONTEND
  # The Jetson repo is unsigned :/ & falky
  apt-get update --allow-unauthenticated --allow-insecure-repositories || echo "couldn't update"
  apt-get upgrade -y
  # This makes debugging less work
  apt-get install -y emacs-nox nano
  apt-get install -y tmate net-tools nmap wireless-tools
  apt-get install -y ssh
  apt-get install -y jq
  # This helps us have working DNS magic
  apt-get install -y avahi-daemon libnss-mdns
  # Stop avahi to keep it from locking anything
  avahi-daemon -k || echo "avahi not started, k"
  # Necessary build magics
  # Note: QEMU means we're running a different kernel
  # than the one we use on the board
  apt install -y linux-headers-raspi2 || echo "Probably not a pi image"
  apt install -y cmake gcc clang
  # Install some build libraries we need
  apt install -y libyaml-cpp-dev libyaml-dev pkg-config libjq-dev
  # For K3s
  apt install -y systemd-sysv
  # On the master we want to make our rook cluster after some time period
  apt-get install at
  # Lets try and install ZFS if we can
  echo debconf zfs-dkms/note-incompatible-licenses select true | debconf-set-selections
  echo debconf common/note-incompatible-licenses select true | debconf-set-selections
  (apt install -y zfs-dkms && apt install -y zfsutils-linux) || echo "Install ZFS wasn't a party, we'll try again later don't worry."
  # I hate netplan
  netplan generate || echo "no netplan, huzzah"
  # iptables needs to use legacy not nftables
  sudo update-alternatives --set iptables /usr/sbin/iptables-legacy || echo "no alt, using current, gl;hf"
  sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy || echo "no alt, using current, gl;hf"
  # We need docker so we can have a party with the GPU later (k3s can use containerd too)
  sudo apt-get remove -y docker docker-engine docker.io containerd runc || echo "k"
  sudo apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common
  # Add docker GPG key
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  # Add the docker repo
  # Until focal is released we just use bionic
#  add-apt-repository \
#   "deb https://download.docker.com/linux/ubuntu \
#   $(lsb_release -cs) \
  #   stable"
  # Jetson unsigned :/
  add-apt-repository --allow-unauthenticated --allow-insecure-repositories  \
   "deb https://download.docker.com/linux/ubuntu \
   bionic \
   stable"
  # The Jetson repo is unsigned :/ & falky
  apt-get update --allow-unauthenticated --allow-insecure-repositories || echo "couldn't update"
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io
  # Start installing falco
  if [ ! -d falco ]; then
    git clone https://github.com/falcosecurity/falco.git
    pushd falco
    git pull
    # This is from nova
    git checkout origin/falco-on-arm
    popd
  fi
  touch /updated_pi
fi
