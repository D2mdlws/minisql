#include "record/row.h"
#include "glog/logging.h"
/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  auto field_num = fields_.size();
  uint32_t bitmap = 0; // 1 in each bit indicates null, 0 indicates not null
  // write field number
  for (uint32_t i = 0; i < field_num; i++) {
    if (fields_[i]->IsNull()) {
      bitmap |= (1 << (field_num - 1 - i));
    }
  }
  // char null_bitmap[field_num / 8 + 1]; // 1 byte for 8 columns
  //
  // memset(null_bitmap, 0, sizeof(null_bitmap));
  //
  // for (uint32_t i = 0; i < field_num; i++) { // set null_bitmap
  //   if (fields_[i]->IsNull()) {
  //     null_bitmap[i / 8] |= (1 << (7 - (i % 8)));
  //   } else {
  //     null_bitmap[i / 8] &= ~(1 << (7 - (i % 8)));
  //   }
  // }

  uint32_t offset = 0;
  // write filed number
  MACH_WRITE_UINT32(buf + offset, field_num);
  offset += sizeof(uint32_t);
  // LOG(INFO) << "field_num in serialization " << field_num << std::endl;

  // write null bitmap
  MACH_WRITE_UINT32(buf+offset, bitmap);
  offset += sizeof(bitmap);

  // write each field
  for (uint32_t i = 0; i < field_num; i++) {
    offset += fields_[i]->SerializeTo(buf + offset);
  }

  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  uint32_t offset = 0;

  // read field number
  uint32_t field_num = MACH_READ_UINT32(buf + offset);
  offset += sizeof(uint32_t);
  // LOG(INFO) << "field_num in deserialization " << field_num << std::endl;
  uint32_t bitmap = 0;
  // char null_bitmap[field_num / 8 + 1];

  // read null bitmap
  bitmap = MACH_READ_UINT32(buf+offset);
  offset += sizeof(uint32_t);

  fields_.resize(field_num);

  // read each field
  for (uint32_t i = 0; i < field_num; i++) {
    if (bitmap & (1 << (field_num - 1 - i))) {
      TypeId type = schema->GetColumn(i)->GetType();
      offset += Field::DeserializeFrom(buf + offset, type, &fields_[i], true);
    } else {
      TypeId type = schema->GetColumn(i)->GetType();
      offset += Field::DeserializeFrom(buf + offset, type, &fields_[i], false);
    }
  }

  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");

  auto field_num = fields_.size();

  uint32_t offset = 0;
  offset += sizeof(uint32_t) + sizeof(uint32_t);
  for (auto i : fields_)
  {
    offset += i->GetSerializedSize();
  }

  return offset;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
