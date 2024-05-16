#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  if (buf == nullptr) {
    LOG(ERROR) << "buf is nullptr";
    return 0;
  }
  uint32_t offset = 0;

  // write magic number
  MACH_WRITE_UINT32(buf + offset, SCHEMA_MAGIC_NUM);
  offset += sizeof(uint32_t);
  // write column size
  MACH_WRITE_UINT32(buf + offset, columns_.size());
  offset += sizeof(uint32_t);

  uint32_t col_offset = 0;
  for (auto &column : columns_) {
    col_offset = column->SerializeTo(buf + offset);
    offset += col_offset;
  }

  return offset;
}

uint32_t Schema::GetSerializedSize() const {

  uint32_t size = 0;
  for (auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  size += 2 * sizeof(uint32_t);  // for magic number
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {

  if (buf == nullptr) {
    LOG(ERROR) << "buf is nullptr";
    return 0;
  }

  uint32_t magic_num = MACH_READ_UINT32(buf);

  if (magic_num != SCHEMA_MAGIC_NUM) {
    LOG(ERROR) << "magic number is not correct";
    return 0;
  }

  uint32_t offset = sizeof(uint32_t);

  uint32_t col_size = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);

  std::vector<Column *> columns;
  // can not use GetSerializedSize() here, because DeserializeFrom func is static
  for (uint32_t i = 0; i < col_size; i++) {
    Column *column = nullptr;
    uint32_t col_offset = Column::DeserializeFrom(buf + offset, column);
    offset += col_offset;
    columns.push_back(column);
  }

  schema = new Schema(columns);

  return offset;
}