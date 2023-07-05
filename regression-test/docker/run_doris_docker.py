import argparse
import logging
import jsonpickle
import os
import os.path
import subprocess
import tempfile
import yaml

DORIS_LOCAL_ROOT = "/tmp/doris"
DORIS_DOCKER_ROOT = "/doris"
MASTER_FE_ID = 1


def get_cluster_path(cluster_name):
    return os.path.join(DORIS_LOCAL_ROOT, cluster_name)


def exec_shell_command(command):
    p = subprocess.Popen(command,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    out = p.communicate()[0].decode('utf-8')
    return p.returncode, out


def get_logger(level=logging.INFO):
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s - %(filename)s - %(lineno)dL - %(levelname)s - %(message)s'
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    logger.setLevel(level)

    return logger


LOG = get_logger()


class Meta(object):

    def __init__(self, cluster_name, subnet_prefix, image):
        self.cluster_name = cluster_name
        self.subnet_prefix = subnet_prefix
        self.image = image
        self.next_fe_id = 1
        self.next_be_id = 1

    def gen_fe_id(self):
        id = self.next_fe_id
        self.next_fe_id += 1
        return id

    def gen_be_id(self):
        id = self.next_be_id
        self.next_be_id += 1
        return id

    @staticmethod
    def load_cluster_meta(cluster_name):
        path = Meta._get_path(cluster_name)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return jsonpickle.loads(f.read())

    def save(self):
        with open(Meta._get_path(self.cluster_name), "w") as f:
            f.write(jsonpickle.dumps(self, indent=2))

    @staticmethod
    def _get_path(cluster_name):
        return os.path.join(get_cluster_path(cluster_name), "meta")


class Node(object):
    TYPE_FE = "fe"
    TYPE_BE = "be"

    def __init__(self, meta, id):
        self.meta = meta
        self.id = id

    def init(self):
        path = self.get_path()
        os.makedirs(path, exist_ok=True)

        # copy to local
        for dir in ["conf", "bin"]:
            cmd = "docker run -v {}:/opt/mount --rm --entrypoint cp {}  -r /doris/{}/{}/ /opt/mount/".format(
                path, self.meta.image, self.node_type(), dir)
            code, output = exec_shell_command(cmd)
            assert code == 0, output

        for sub_dir in self.expose_sub_dirs():
            os.makedirs(os.path.join(path, sub_dir), exist_ok=True)

    def node_type(self):
        raise Exception("No implement")

    def expose_sub_dirs(self):
        return ["conf", "log", "bin"]

    def get_name(self):
        return "{}-{}".format(self.node_type(), self.id)

    def get_path(self):
        return os.path.join(get_cluster_path(self.meta.cluster_name),
                            self.get_name())

    def get_ip(self):
        num3 = None
        if self.node_type() == Node.TYPE_FE:
            num3 = 1
        elif self.node_type() == Node.TYPE_BE:
            num3 = 2
        else:
            raise Exception("Unknown node type: {}".format(self.node_type()))
        return "{}.0.{}.{}".format(self.meta.subnet_prefix, num3, self.id)

    def get_master_fe(self):
        return FE(self.meta, MASTER_FE_ID)

    # {doris_home}/{fe, be}
    def docker_node_home(self):
        return os.path.join(DORIS_DOCKER_ROOT, self.node_type())

    def docker_service(self):
        return "{}-{}".format(self.meta.cluster_name, self.get_name())

    def docker_command(self):
        raise Exception("No implement")

    def docker_env(self):
        return []

    def docker_ports(self):
        raise Exception("No implement")

    def docker_volumns(self):
        raise Exception("No implement")

    def docker_depends_on(self):
        if self.node_type() == Node.TYPE_FE and self.id == MASTER_FE_ID:
            return []
        else:
            return [self.get_master_fe().docker_service()]

    def compose(self):
        return {
            "cap_add": ["SYS_PTRACE"],
            "command":
            self.docker_command(),
            "container_name":
            self.docker_service(),
            "environment":
            self.docker_env(),
            "depends_on":
            self.docker_depends_on(),
            "image":
            self.meta.image,
            "networks": {
                self.meta.cluster_name: {
                    "ipv4_address": self.get_ip(),
                }
            },
            "ports":
            self.docker_ports(),
            "ulimits": {
                "core": -1
            },
            "security_opt": ["seccomp:unconfined"],
            "volumes": [
                "{}:{}".format(os.path.join(self.get_path(), sub_dir),
                               os.path.join(self.docker_node_home(), sub_dir))
                for sub_dir in self.expose_sub_dirs()
            ],
        }


class FE(Node):

    def docker_command(self):
        return [
            "bash",
            "{}/bin/{}".format(
                self.docker_node_home(), "start_fe.sh"
                if self.id == MASTER_FE_ID else "follower_start_fe.sh"),
        ]

        if self.id != MASTER_FE_ID:
            commands.append("--helper")
            commands.append("{}:9010".format(self.get_master_fe().get_ip()))

        return commands

    def docker_ports(self):
        return [8030, 9010, 9020, 9030]

    def node_type(self):
        return Node.TYPE_FE

    def init(self):
        if self.id != MASTER_FE_ID:
            os.makedirs(os.path.join(self.get_path(), "bin"), exist_ok=True)
            be_out = "{}/fe/log/fe.out".format(DORIS_DOCKER_ROOT)
            old_start_fe = "{}/bin/start_fe.sh".format(self.get_path())
            follower_start_fe = "{}/bin/follower_start_fe.sh".format(
                self.get_path())
            master_fe_ip = self.get_master_fe().get_ip()
            with open(follower_start_fe, "w") as f:
                f.write("""
while true; do
    output=`mysql -P 9030 -h {} -u root --execute "ALTER SYSTEM ADD FOLLOWER '{}:9010';" 2>&1`
    res=$?
    echo "`date`\n$output" >> {}
    [ $res -eq 0 ] && break
    (echo $output | grep "frontend already exists") && break
    sleep 1
done
bash {}/fe/bin/start_fe.sh --helper {}:9010
""".format(master_fe_ip, self.get_ip(), be_out, DORIS_DOCKER_ROOT,
                master_fe_ip))
        super().init()


class BE(Node):

    def docker_command(self):
        return [
            "bash",
            os.path.join(self.docker_node_home(), "bin/new_start_be.sh"),
        ]

    def docker_ports(self):
        return [8040, 8060, 9050, 9060]

    def node_type(self):
        return Node.TYPE_BE

    def init(self):
        os.makedirs(os.path.join(self.get_path(), "bin"), exist_ok=True)
        be_out = "{}/be/log/be.out".format(DORIS_DOCKER_ROOT)
        old_start_be = "{}/bin/start_be.sh".format(self.get_path())
        new_start_be = "{}/bin/new_start_be.sh".format(self.get_path())
        with open(new_start_be, "w") as f:
            f.write("""
while true; do
    output=`mysql -P 9030 -h {} -u root --execute "ALTER SYSTEM ADD BACKEND '{}:9050';" 2>&1`
    res=$?
    echo "`date`\n$output" >> {}
    [ $res -eq 0 ] && break
    (echo $output | grep "Same backend already exists") && break
    sleep 1
done
bash {}/be/bin/start_be.sh
""".format(self.get_master_fe().get_ip(), self.get_ip(), be_out,
            DORIS_DOCKER_ROOT))

        super().init()


class Cluster(object):

    def __init__(self, image):
        cluster_name = os.path.basename(
            tempfile.mkdtemp("", "doris.", DORIS_LOCAL_ROOT))
        subnet_prefix = self._gen_subnet_prefix()

        self.meta = Meta(cluster_name, subnet_prefix, image)
        self.frontends = []
        self.backends = []

    def _gen_subnet_prefix(self):
        used_subnet_prefix = {}
        for cluster_name in os.listdir(DORIS_LOCAL_ROOT):
            meta = Meta.load_cluster_meta(cluster_name)
            if meta:
                used_subnet_prefix[meta.subnet_prefix] = True
        for i in range(11, 191):
            if not used_subnet_prefix.get(i, False):
                return i
        raise Exception("Failed to init subnet")

    def get_path(self):
        return get_cluster_path(self.meta.cluster_name)

    def get_cluster_name(self):
        return self.meta.cluster_name

    def add_fe(self):
        id = self.meta.gen_fe_id()
        if id > 255:
            raise Exception("fe id exceed 255")
        fe = FE(self.meta, id)
        fe.init()
        self.frontends.append(fe)

    def add_be(self):
        id = self.meta.gen_be_id()
        if id > 255:
            raise Exception("be id exceed 255")
        be = BE(self.meta, id)
        be.init()
        self.backends.append(be)

    def save_meta(self):
        self.meta.save()

    def save_compose(self):
        compose = {
            "version": "3",
            "networks": {
                self.meta.cluster_name: {
                    "driver": "bridge",
                    "ipam": {
                        "config": [{
                            "subnet":
                            "{}.0.0.0/8".format(self.meta.subnet_prefix)
                        }]
                    }
                }
            },
            "services": {
                node.docker_service(): node.compose()
                for node in self.frontends + self.backends
            }
        }
        with open(self.get_compose_path(), "w") as f:
            f.write(yaml.dump(compose))

    def get_compose_path(self):
        return os.path.join(self.get_path(), "docker-compose.yml")

    def save(self):
        self.save_meta()
        self.save_compose()

    def start(self):
        cmd = "docker-compose -f {} up -d".format(self.get_compose_path())
        code, output = exec_shell_command(cmd)
        assert code == 0, output


def run(args):
    cluster = Cluster(args.IMAGE)
    LOG.info("New cluster {}".format(cluster.get_cluster_name()))
    for i in range(args.fe):
        cluster.add_fe()
    for i in range(args.be):
        cluster.add_be()
    cluster.save()
    cluster.start()
    LOG.info("Start cluster {} succ".format(cluster.get_cluster_name()))


def parse_args():
    ap = argparse.ArgumentParser(description="")
    sub_aps = ap.add_subparsers(dest="command")

    ap_run = sub_aps.add_parser("run", help="run a doris cluster")
    ap_run.add_argument("IMAGE", help="specify docker image")
    ap_run.add_argument("--fe", type=int, default=3, help="specify fe count")
    ap_run.add_argument("--be", type=int, default=3, help="specify be count")

    return ap.format_usage(), ap.format_help(), ap.parse_args()


def main():
    usage, _, args = parse_args()
    if args.command == "run":
        return run(args)
    else:
        print(usage)
        return -1


if __name__ == '__main__':
    main()
