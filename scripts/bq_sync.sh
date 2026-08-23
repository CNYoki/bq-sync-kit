#!/usr/bin/env bash
# 供 cron 调用的 bq_sync_kit 包装脚本。
# 用法: bq_sync.sh [bq-sync-kit run 的任意参数]
#   bq_sync.sh                      同步所有项目
#   bq_sync.sh --project douyin     只同步一个项目
#   bq_sync.sh --dry-run            只看会同步哪些文件
set -Eeuo pipefail

# 默认按脚本自身位置推断项目根目录，也可以用环境变量覆盖。
KIT_HOME="${KIT_HOME:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
BIN="${BQ_SYNC_BIN:-${KIT_HOME}/.venv/bin/bq-sync-kit}"
CONFIG="${BQ_SYNC_CONFIG:-${KIT_HOME}/config.yaml}"
ENV_FILE="${BQ_SYNC_ENV_FILE:-${KIT_HOME}/.env}"
LOG_DIR="${BQ_SYNC_LOG_DIR:-${KIT_HOME}/logs}"
LOCK_FILE="${BQ_SYNC_LOCK_FILE:-/tmp/bq_sync_kit.lock}"
LOG_KEEP_DAYS="${BQ_SYNC_LOG_KEEP_DAYS:-30}"

mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/sync-$(date +%Y-%m-%d).log"
exec >>"${LOG_FILE}" 2>&1

# 密码之类的敏感值放 .env（chmod 600），配置里用 ${VAR} 引用。
if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
fi

echo "=== $(date '+%F %T %Z') 启动: $* ==="

# 单机层面防重入。库里还有 MySQL GET_LOCK 做跨主机互斥，
# 但那条路径会以 exit 1 报错退出；本地用 flock 挡住更干净。
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    echo "上一轮同步仍在运行，本次跳过"
    exit 0
fi

start=$(date +%s)
set +e
"${BIN}" --config "${CONFIG}" run "$@"
code=$?
set -e
elapsed=$(( $(date +%s) - start ))

case "${code}" in
    0) echo "=== 完成，耗时 ${elapsed}s ===" ;;
    2) echo "=== 配置错误（exit 2），耗时 ${elapsed}s ===" ;;
    *) echo "=== 失败（exit ${code}），耗时 ${elapsed}s ===" ;;
esac

find "${LOG_DIR}" -name 'sync-*.log' -type f -mtime "+${LOG_KEEP_DAYS}" -delete 2>/dev/null || true
exit "${code}"
