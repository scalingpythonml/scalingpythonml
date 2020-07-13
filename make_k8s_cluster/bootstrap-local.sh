#!/bin/bash

source bootstrap-funcs.sh

prepare_local

cleanup_ubuntu_mounts
if [ ! -f images/ubuntu-arm64-customized.img ]; then
  cp images/ubuntu-arm64.img images/ubuntu-arm64-customized.img
  setup_ubuntu_server_img images/ubuntu-arm64-customized.img
  # Extend the image, first check the current FS
  sudo umount /dev/mapper/${partition} || echo "not mounted :)"
  sync
  sleep 1
  sudo e2fsck -f /dev/mapper/${partition}
  sudo kpartx -dv images/ubuntu-arm64-customized.img
  sync
  sleep 5
  resize_partition images/ubuntu-arm64-customized.img 2 ${PI_TARGET_SIZE}
  setup_ubuntu_server_img images/ubuntu-arm64-customized.img
  setup_ubuntu_mounts
  enable_chroot
  update_ubuntu
  cleanup_ubuntu_mounts
  sudo kpartx -dv images/ubuntu-arm64-customized.img
  sync
  sleep 5
fi
echo "Baking leader/worker images"
# Setup K3s
if [ ! -f images/ubuntu-arm64-leader.img ]; then
  # Setup the leader
  cp images/ubuntu-arm64-customized.img images/ubuntu-arm64-leader.img
  setup_ubuntu_server_img images/ubuntu-arm64-leader.img
  setup_ubuntu_mounts
  copy_ssh_keys
  config_system
  ${COPY_COMMAND} leaderhost ubuntu-image/etc/hostname
  ${COPY_COMMAND} firstboot.sh ubuntu-image/etc/init.d/firstboot
  ${COPY_COMMAND} first_run_leader.sh ubuntu-image/do_firstboot.sh
  ${RUN_DEST_CMD} update-rc.d  firstboot defaults
  # The leader needs to have rook checked out
  ${COPY_COMMAND} -af rook ubuntu-image/
  ${COPY_COMMAND} setup_*.sh ubuntu-image/
  # Note operator.yaml change v2.1.1 to v2.1.1-arm64
  ${COPY_COMMAND} rook_*.yaml ubuntu-image/rook/rook/cluster/examples/kubernetes/ceph/
  # The leader has a worker counter file
  ${COPY_COMMAND} worker_counter.txt ubuntu-image/
  cleanup_misc
  cleanup_ubuntu_mounts
  sudo kpartx -dv images/ubuntu-arm64-leader.img
  # Setup the worker
  cp images/ubuntu-arm64-customized.img images/ubuntu-arm64-worker.img
  setup_ubuntu_server_img images/ubuntu-arm64-worker.img
  setup_ubuntu_mounts
  copy_ssh_keys
  config_system
  ${COPY_COMMAND} firstboot.sh ubuntu-image/etc/init.d/firstboot
  ${COPY_COMMAND} first_run_worker.sh ubuntu-image/do_firstboot.sh
  ${RUN_DEST_CMD} update-rc.d  firstboot defaults
  cleanup_misc
  cleanup_ubuntu_mounts
  sudo kpartx -dv images/ubuntu-arm64-worker.img
  sync
fi
echo "Baking jetson nano worker image"
unset firmware_boot_partition
if [ -z "${jpid}" ]; then
  wait ${jpid} || echo "dl done"
fi
if [ ! -f images/sd-blob-b01.img ]; then
  pushd images; unzip jetson-nano.zip; popd
fi
if [ ! -f images/jetson-nano-custom.img ]; then
  cp images/sd-blob-b01.img images/jetson-nano-custom.img
  setup_jetson_img images/jetson-nano-custom.img
  setup_ubuntu_mounts
  cleanup_ubuntu_mounts
  resize_partition images/jetson-nano-custom.img 1 ${JETSON_TARGET_SIZE}
  setup_jetson_img images/jetson-nano-custom.img
  setup_ubuntu_mounts
  enable_chroot
  update_ubuntu
  config_system
  if [ -f jetson_docker_daemon.json.custom ]; then
    ${COPY_COMMAND} jetson_docker_daemon.json.custom ubuntu-image/etc/docker/daemon.json
  else
    ${COPY_COMMAND} jetson_docker_daemon.json ubuntu-image/etc/docker/daemon.json
  fi
  ${COPY_COMMAND} firstboot.sh ubuntu-image/etc/init.d/firstboot
  ${COPY_COMMAND} first_run_worker.sh ubuntu-image/do_firstboot.sh
  ${COPY_COMMAND} setup_k3s_worker_gpu.sh ubuntu-image/setup_k3s_worker.sh
  ${RUN_DEST_CMD} update-rc.d  firstboot defaults
  copy_ssh_keys
  cleanup_ubuntu_mounts
  sudo kpartx -dv images/jetson-nano-custom.img
fi