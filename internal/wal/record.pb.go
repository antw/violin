// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.17.3
// source: internal/wal/record.proto

package wal

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Record struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Txid uint64 `protobuf:"varint,1,opt,name=txid,proto3" json:"txid,omitempty"`
	// Types that are assignable to Record:
	//	*Record_Upsert
	//	*Record_Delete
	Record isRecord_Record `protobuf_oneof:"record"`
}

func (x *Record) Reset() {
	*x = Record{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_wal_record_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Record) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Record) ProtoMessage() {}

func (x *Record) ProtoReflect() protoreflect.Message {
	mi := &file_internal_wal_record_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Record.ProtoReflect.Descriptor instead.
func (*Record) Descriptor() ([]byte, []int) {
	return file_internal_wal_record_proto_rawDescGZIP(), []int{0}
}

func (x *Record) GetTxid() uint64 {
	if x != nil {
		return x.Txid
	}
	return 0
}

func (m *Record) GetRecord() isRecord_Record {
	if m != nil {
		return m.Record
	}
	return nil
}

func (x *Record) GetUpsert() *Upsert {
	if x, ok := x.GetRecord().(*Record_Upsert); ok {
		return x.Upsert
	}
	return nil
}

func (x *Record) GetDelete() *Delete {
	if x, ok := x.GetRecord().(*Record_Delete); ok {
		return x.Delete
	}
	return nil
}

type isRecord_Record interface {
	isRecord_Record()
}

type Record_Upsert struct {
	Upsert *Upsert `protobuf:"bytes,2,opt,name=upsert,proto3,oneof"`
}

type Record_Delete struct {
	Delete *Delete `protobuf:"bytes,3,opt,name=delete,proto3,oneof"`
}

func (*Record_Upsert) isRecord_Record() {}

func (*Record_Delete) isRecord_Record() {}

type Upsert struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Upsert) Reset() {
	*x = Upsert{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_wal_record_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Upsert) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Upsert) ProtoMessage() {}

func (x *Upsert) ProtoReflect() protoreflect.Message {
	mi := &file_internal_wal_record_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Upsert.ProtoReflect.Descriptor instead.
func (*Upsert) Descriptor() ([]byte, []int) {
	return file_internal_wal_record_proto_rawDescGZIP(), []int{1}
}

func (x *Upsert) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Upsert) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

type Delete struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
}

func (x *Delete) Reset() {
	*x = Delete{}
	if protoimpl.UnsafeEnabled {
		mi := &file_internal_wal_record_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Delete) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Delete) ProtoMessage() {}

func (x *Delete) ProtoReflect() protoreflect.Message {
	mi := &file_internal_wal_record_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Delete.ProtoReflect.Descriptor instead.
func (*Delete) Descriptor() ([]byte, []int) {
	return file_internal_wal_record_proto_rawDescGZIP(), []int{2}
}

func (x *Delete) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

var File_internal_wal_record_proto protoreflect.FileDescriptor

var file_internal_wal_record_proto_rawDesc = []byte{
	0x0a, 0x19, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x77, 0x61, 0x6c, 0x2f, 0x72,
	0x65, 0x63, 0x6f, 0x72, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x6c, 0x0a, 0x06, 0x52,
	0x65, 0x63, 0x6f, 0x72, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x78, 0x69, 0x64, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x04, 0x52, 0x04, 0x74, 0x78, 0x69, 0x64, 0x12, 0x21, 0x0a, 0x06, 0x75, 0x70, 0x73,
	0x65, 0x72, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x55, 0x70, 0x73, 0x65,
	0x72, 0x74, 0x48, 0x00, 0x52, 0x06, 0x75, 0x70, 0x73, 0x65, 0x72, 0x74, 0x12, 0x21, 0x0a, 0x06,
	0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x07, 0x2e, 0x44,
	0x65, 0x6c, 0x65, 0x74, 0x65, 0x48, 0x00, 0x52, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x42,
	0x08, 0x0a, 0x06, 0x72, 0x65, 0x63, 0x6f, 0x72, 0x64, 0x22, 0x30, 0x0a, 0x06, 0x55, 0x70, 0x73,
	0x65, 0x72, 0x74, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x1a, 0x0a, 0x06, 0x44,
	0x65, 0x6c, 0x65, 0x74, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x42, 0x25, 0x5a, 0x23, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x6e, 0x74, 0x77, 0x2f, 0x76, 0x69, 0x6f, 0x6c, 0x69,
	0x6e, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x77, 0x61, 0x6c, 0x62, 0x06,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_internal_wal_record_proto_rawDescOnce sync.Once
	file_internal_wal_record_proto_rawDescData = file_internal_wal_record_proto_rawDesc
)

func file_internal_wal_record_proto_rawDescGZIP() []byte {
	file_internal_wal_record_proto_rawDescOnce.Do(func() {
		file_internal_wal_record_proto_rawDescData = protoimpl.X.CompressGZIP(file_internal_wal_record_proto_rawDescData)
	})
	return file_internal_wal_record_proto_rawDescData
}

var file_internal_wal_record_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_internal_wal_record_proto_goTypes = []interface{}{
	(*Record)(nil), // 0: Record
	(*Upsert)(nil), // 1: Upsert
	(*Delete)(nil), // 2: Delete
}
var file_internal_wal_record_proto_depIdxs = []int32{
	1, // 0: Record.upsert:type_name -> Upsert
	2, // 1: Record.delete:type_name -> Delete
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_internal_wal_record_proto_init() }
func file_internal_wal_record_proto_init() {
	if File_internal_wal_record_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_internal_wal_record_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Record); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_wal_record_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Upsert); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_internal_wal_record_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Delete); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_internal_wal_record_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*Record_Upsert)(nil),
		(*Record_Delete)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_internal_wal_record_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_internal_wal_record_proto_goTypes,
		DependencyIndexes: file_internal_wal_record_proto_depIdxs,
		MessageInfos:      file_internal_wal_record_proto_msgTypes,
	}.Build()
	File_internal_wal_record_proto = out.File
	file_internal_wal_record_proto_rawDesc = nil
	file_internal_wal_record_proto_goTypes = nil
	file_internal_wal_record_proto_depIdxs = nil
}
