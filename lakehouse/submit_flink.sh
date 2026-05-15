#!/bin/bash
# =============================================================
#  submit_flink.sh — CHỈ submit Flink Jobs (tách riêng khỏi deploy)
#  Dùng khi: sửa SQL xong muốn chạy lại mà không cần deploy lại hạ tầng
#
#  Cách dùng:
#    ./submit_flink.sh                        # Submit file SQL mặc định
#    ./submit_flink.sh path/to/custom.sql     # Submit file SQL tùy chọn
#    ./submit_flink.sh --cancel               # Hủy tất cả Job đang chạy
#    ./submit_flink.sh --restart              # Hủy cũ + submit lại
# =============================================================
set -e

source "$(dirname "$0")/pipeline_common.sh"

# Load .env
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

PROJECT_NAME=${mysql_database:-"real_admin"}
DEFAULT_SQL="generated/${PROJECT_NAME}/pipeline_from_db.sql"

ACTION="$1"

case "$ACTION" in
    --cancel)
        echo "🛑 Đang hủy tất cả Flink Jobs..."
        common::cleanup_flink_jobs
        echo "✅ Đã hủy xong."
        ;;
    --restart)
        SQL_FILE="${2:-$DEFAULT_SQL}"
        echo "🔄 Restart: Hủy Jobs cũ + Submit lại..."
        common::cleanup_flink_jobs
        sleep 3
        echo ">>> Submit: $SQL_FILE"
        common::submit_flink_sql "$SQL_FILE"
        echo "✅ Đã restart xong."
        ;;
    --help)
        echo "Cách dùng:"
        echo "  ./submit_flink.sh                    Submit file SQL mặc định"
        echo "  ./submit_flink.sh file.sql           Submit file SQL tùy chọn"
        echo "  ./submit_flink.sh --cancel           Hủy tất cả Jobs"
        echo "  ./submit_flink.sh --restart           Hủy + submit lại"
        echo "  ./submit_flink.sh --restart file.sql  Hủy + submit file tùy chọn"
        ;;
    *)
        SQL_FILE="${ACTION:-$DEFAULT_SQL}"
        if [ ! -f "$SQL_FILE" ]; then
            echo "❌ Không tìm thấy: $SQL_FILE"
            exit 1
        fi
        echo ">>> Submit Flink SQL: $SQL_FILE"
        common::submit_flink_sql "$SQL_FILE"
        echo "✅ Đã submit xong."
        ;;
esac
