#!/bin/bash
# Script kiểm tra JAR files không được sử dụng trong project

echo "=== Phân tích JAR files trong project ==="
echo "📂 Quét thư mục: lakehouse/ và student/"
echo ""

# Tìm tất cả file JAR trong lakehouse và student
echo "📦 Tìm tất cả file JAR..."
find lakehouse student -name "*.jar" -type f 2>/dev/null > /tmp/all_jars.txt
TOTAL_JARS=$(wc -l < /tmp/all_jars.txt)
echo "   Tổng số: $TOTAL_JARS files"
echo ""

# Tạo danh sách JAR được reference trong code
echo "🔍 Tìm JAR được reference trong code..."
grep -r "\.jar" --include="*.sh" --include="*.py" --include="*.sql" --include="*.xml" --include="*.properties" --include="*.env" lakehouse student 2>/dev/null | \
  grep -oE '[a-zA-Z0-9_.-]+\.jar' | sort -u > /tmp/referenced_jars.txt
REFERENCED=$(wc -l < /tmp/referenced_jars.txt)
echo "   Được reference: $REFERENCED files"
echo ""

# So sánh và tìm JAR không được sử dụng
echo "❌ JAR files KHÔNG được sử dụng:"
echo "================================"
UNUSED_COUNT=0
UNUSED_SIZE=0

while IFS= read -r jar_path; do
  jar_name=$(basename "$jar_path")
  
  # Kiểm tra xem JAR có được reference không
  if ! grep -q "$jar_name" /tmp/referenced_jars.txt; then
    size=$(du -h "$jar_path" | cut -f1)
    size_bytes=$(du -b "$jar_path" | cut -f1)
    echo "  - $jar_path ($size)"
    UNUSED_COUNT=$((UNUSED_COUNT + 1))
    UNUSED_SIZE=$((UNUSED_SIZE + size_bytes))
  fi
done < /tmp/all_jars.txt

echo ""
echo "================================"
echo "📊 Tổng kết:"
echo "   Tổng JAR: $TOTAL_JARS"
echo "   Được sử dụng: $((TOTAL_JARS - UNUSED_COUNT))"
echo "   KHÔNG sử dụng: $UNUSED_COUNT"
echo "   Dung lượng có thể tiết kiệm: $(numfmt --to=iec-i --suffix=B $UNUSED_SIZE 2>/dev/null || echo "$UNUSED_SIZE bytes")"
echo ""

# Tạo file .gitignore suggestion
if [ $UNUSED_COUNT -gt 0 ]; then
  echo "💡 Gợi ý: Thêm vào .gitignore:"
  echo "================================"
  while IFS= read -r jar_path; do
    jar_name=$(basename "$jar_path")
    if ! grep -q "$jar_name" /tmp/referenced_jars.txt; then
      echo "$jar_path"
    fi
  done < /tmp/all_jars.txt
  echo ""
  echo "💡 Hoặc xóa các file không dùng:"
  echo "================================"
  echo "while IFS= read -r f; do rm \"\$f\"; done < <(cat <<'EOF'"
  while IFS= read -r jar_path; do
    jar_name=$(basename "$jar_path")
    if ! grep -q "$jar_name" /tmp/referenced_jars.txt; then
      echo "$jar_path"
    fi
  done < /tmp/all_jars.txt
  echo "EOF"
  echo ")"
fi

# Cleanup
rm -f /tmp/all_jars.txt /tmp/referenced_jars.txt
