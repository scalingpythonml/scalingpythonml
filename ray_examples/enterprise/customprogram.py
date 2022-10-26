import ray
import subprocess

ray.init(address='ray://<your IP>:10001')
@ray.remote(num_cpus=6)
def runDocker(cmd):
    with open("result.txt", "w") as output:
        result = subprocess.run(
            cmd,
            shell=True, # pass single string to shell, let it handle.
            stdout=output,
            stderr=output
    )
    print(f"return code {result.returncode}")
    with open("result.txt", "r") as output:
        log = output.read()
    return log

cmd='docker run --rm busybox echo "Hello world"'
result=runDocker.remote(cmd)
print(f"result: {ray.get(result)}")