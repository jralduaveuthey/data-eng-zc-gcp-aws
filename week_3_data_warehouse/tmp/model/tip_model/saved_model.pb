??
??
D
AddV2
x"T
y"T
z"T"
Ttype:
2	??
^
AssignVariableOp
resource
value"dtype"
dtypetype"
validate_shapebool( ?
N
Cast	
x"SrcT	
y"DstT"
SrcTtype"
DstTtype"
Truncatebool( 
h
ConcatV2
values"T*N
axis"Tidx
output"T"
Nint(0"	
Ttype"
Tidxtype0:
2	
8
Const
output"dtype"
valuetensor"
dtypetype
R
Equal
x"T
y"T
z
"	
Ttype"$
incompatible_shape_errorbool(?
W

ExpandDims

input"T
dim"Tdim
output"T"	
Ttype"
Tdimtype0:
2	
q
GatherNd
params"Tparams
indices"Tindices
output"Tparams"
Tparamstype"
Tindicestype:
2	
?
HashTableV2
table_handle"
	containerstring "
shared_namestring "!
use_node_name_sharingbool( "
	key_dtypetype"
value_dtypetype?
.
Identity

input"T
output"T"	
Ttype
?
InitializeTableFromTextFileV2
table_handle
filename"
	key_indexint(0?????????"
value_indexint(0?????????"+

vocab_sizeint?????????(0?????????"
	delimiterstring	"
offsetint ?
w
LookupTableFindV2
table_handle
keys"Tin
default_value"Tout
values"Tout"
Tintype"
Touttype?
q
MatMul
a"T
b"T
product"T"
transpose_abool( "
transpose_bbool( "
Ttype:

2	
?
MergeV2Checkpoints
checkpoint_prefixes
destination_prefix"
delete_old_dirsbool("
allow_missing_filesbool( ?

NoOp
U
NotEqual
x"T
y"T
z
"	
Ttype"$
incompatible_shape_errorbool(?
?
OneHot
indices"TI	
depth
on_value"T
	off_value"T
output"T"
axisint?????????"	
Ttype"
TItype0	:
2	
M
Pack
values"T*N
output"T"
Nint(0"	
Ttype"
axisint 
C
Placeholder
output"dtype"
dtypetype"
shapeshape:
X
PlaceholderWithDefault
input"dtype
output"dtype"
dtypetype"
shapeshape
@
ReadVariableOp
resource
value"dtype"
dtypetype?
[
Reshape
tensor"T
shape"Tshape
output"T"	
Ttype"
Tshapetype0:
2	
o
	RestoreV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0?
l
SaveV2

prefix
tensor_names
shape_and_slices
tensors2dtypes"
dtypes
list(type)(0?
?
Select
	condition

t"T
e"T
output"T"	
Ttype
A
SelectV2
	condition

t"T
e"T
output"T"	
Ttype
d
Shape

input"T&
output"out_type??out_type"	
Ttype"
out_typetype0:
2	
H
ShardedFilename
basename	
shard

num_shards
filename
?
SparseToDense
sparse_indices"Tindices
output_shape"Tindices
sparse_values"T
default_value"T

dense"T"
validate_indicesbool("	
Ttype"
Tindicestype:
2	
@
StaticRegexFullMatch	
input

output
"
patternstring
?
StridedSlice

input"T
begin"Index
end"Index
strides"Index
output"T"	
Ttype"
Indextype:
2	"

begin_maskint "
end_maskint "
ellipsis_maskint "
new_axis_maskint "
shrink_axis_maskint 
N

StringJoin
inputs*N

output"
Nint(0"
	separatorstring 
?
Sum

input"T
reduction_indices"Tidx
output"T"
	keep_dimsbool( ""
Ttype:
2	"
Tidxtype0:
2	
c
Tile

input"T
	multiples"
Tmultiples
output"T"	
Ttype"

Tmultiplestype0:
2	
?
VarHandleOp
resource"
	containerstring "
shared_namestring "
dtypetype"
shapeshape"#
allowed_deviceslist(string)
 ?
9
VarIsInitializedOp
resource
is_initialized
?
G
Where

input"T	
index	"'
Ttype0
:
2	
"serve*2.12.02unknown8??
f
fare_amountPlaceholder*#
_output_shapes
:?????????*
dtype0*
shape:?????????
j
passenger_countPlaceholder*#
_output_shapes
:?????????*
dtype0*
shape:?????????
g
tolls_amountPlaceholder*#
_output_shapes
:?????????*
dtype0*
shape:?????????
h
trip_distancePlaceholder*#
_output_shapes
:?????????*
dtype0*
shape:?????????
g
DOLocationIDPlaceholder*#
_output_shapes
:?????????*
dtype0*
shape:?????????
H
Equal/yConst*
_output_shapes
: *
dtype0*
valueB B 
S
EqualEqualDOLocationIDEqual/y*
T0*#
_output_shapes
:?????????
j
ConstConst*
_output_shapes
:*
dtype0*1
value(B&B__empty_string_replacement__
O
ShapeShapeDOLocationID*
T0*
_output_shapes
::??
H
TileTileConstShape*
T0*#
_output_shapes
:?????????
]
SelectV2SelectV2EqualTileDOLocationID*
T0*#
_output_shapes
:?????????
g
PULocationIDPlaceholder*#
_output_shapes
:?????????*
dtype0*
shape:?????????
J
	Equal_1/yConst*
_output_shapes
: *
dtype0*
valueB B 
W
Equal_1EqualPULocationID	Equal_1/y*
T0*#
_output_shapes
:?????????
l
Const_1Const*
_output_shapes
:*
dtype0*1
value(B&B__empty_string_replacement__
Q
Shape_1ShapePULocationID*
T0*
_output_shapes
::??
N
Tile_1TileConst_1Shape_1*
T0*#
_output_shapes
:?????????
c

SelectV2_1SelectV2Equal_1Tile_1PULocationID*
T0*#
_output_shapes
:?????????
g
payment_typePlaceholder*#
_output_shapes
:?????????*
dtype0*
shape:?????????
J
	Equal_2/yConst*
_output_shapes
: *
dtype0*
valueB B 
W
Equal_2Equalpayment_type	Equal_2/y*
T0*#
_output_shapes
:?????????
l
Const_2Const*
_output_shapes
:*
dtype0*1
value(B&B__empty_string_replacement__
Q
Shape_2Shapepayment_type*
T0*
_output_shapes
::??
N
Tile_2TileConst_2Shape_2*
T0*#
_output_shapes
:?????????
c

SelectV2_2SelectV2Equal_2Tile_2payment_type*
T0*#
_output_shapes
:?????????
|
1input_layer/DOLocationID_indicator/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
?????????
?
-input_layer/DOLocationID_indicator/ExpandDims
ExpandDimsSelectV21input_layer/DOLocationID_indicator/ExpandDims/dim*
T0*'
_output_shapes
:?????????
?
Ainput_layer/DOLocationID_indicator/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB B 
?
;input_layer/DOLocationID_indicator/to_sparse_input/NotEqualNotEqual-input_layer/DOLocationID_indicator/ExpandDimsAinput_layer/DOLocationID_indicator/to_sparse_input/ignore_value/x*
T0*'
_output_shapes
:?????????
?
:input_layer/DOLocationID_indicator/to_sparse_input/indicesWhere;input_layer/DOLocationID_indicator/to_sparse_input/NotEqual*'
_output_shapes
:?????????
?
9input_layer/DOLocationID_indicator/to_sparse_input/valuesGatherNd-input_layer/DOLocationID_indicator/ExpandDims:input_layer/DOLocationID_indicator/to_sparse_input/indices*
Tindices0	*
Tparams0*#
_output_shapes
:?????????
?
>input_layer/DOLocationID_indicator/to_sparse_input/dense_shapeShape-input_layer/DOLocationID_indicator/ExpandDims*
T0*
_output_shapes
:*
out_type0	:??
?
Linput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/asset_pathConst"/device:CPU:**
_output_shapes
: *
dtype0*?
value?B? B?/cns/jd-d/home/cloud-dataengine-materialize-tempstore/serving-materialize/serving.shard-mals/1/1131/ttl=12h/c86656b7e65654a6/assets/DOLocationID.txt
?
Ginput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/ConstConst*
_output_shapes
: *
dtype0	*
valueB	 R
?????????
?
Linput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/hash_tableHashTableV2*
_output_shapes
: *
	key_dtype0*?
shared_name??hash_table_/cns/jd-d/home/cloud-dataengine-materialize-tempstore/serving-materialize/serving.shard-mals/1/1131/ttl=12h/c86656b7e65654a6/assets/DOLocationID.txt_262_-2_-1*
value_dtype0	
?
jinput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2InitializeTableFromTextFileV2Linput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/hash_tableLinput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/asset_path*&
 _has_manual_control_dependencies(*
	key_index?????????*
value_index?????????*

vocab_size?
?
Finput_layer/DOLocationID_indicator/hash_table_Lookup/LookupTableFindV2LookupTableFindV2Linput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/hash_table9input_layer/DOLocationID_indicator/to_sparse_input/valuesGinput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/Const*	
Tin0*

Tout0	*#
_output_shapes
:?????????
?
>input_layer/DOLocationID_indicator/SparseToDense/default_valueConst*
_output_shapes
: *
dtype0	*
valueB	 R
?????????
?
0input_layer/DOLocationID_indicator/SparseToDenseSparseToDense:input_layer/DOLocationID_indicator/to_sparse_input/indices>input_layer/DOLocationID_indicator/to_sparse_input/dense_shapeFinput_layer/DOLocationID_indicator/hash_table_Lookup/LookupTableFindV2>input_layer/DOLocationID_indicator/SparseToDense/default_value*
T0	*
Tindices0	*'
_output_shapes
:?????????
u
0input_layer/DOLocationID_indicator/one_hot/ConstConst*
_output_shapes
: *
dtype0*
valueB
 *  ??
w
2input_layer/DOLocationID_indicator/one_hot/Const_1Const*
_output_shapes
: *
dtype0*
valueB
 *    
s
0input_layer/DOLocationID_indicator/one_hot/depthConst*
_output_shapes
: *
dtype0*
value
B :?
?
*input_layer/DOLocationID_indicator/one_hotOneHot0input_layer/DOLocationID_indicator/SparseToDense0input_layer/DOLocationID_indicator/one_hot/depth0input_layer/DOLocationID_indicator/one_hot/Const2input_layer/DOLocationID_indicator/one_hot/Const_1*
T0*,
_output_shapes
:??????????
?
8input_layer/DOLocationID_indicator/Sum/reduction_indicesConst*
_output_shapes
:*
dtype0*
valueB:
?????????
?
&input_layer/DOLocationID_indicator/SumSum*input_layer/DOLocationID_indicator/one_hot8input_layer/DOLocationID_indicator/Sum/reduction_indices*
T0*(
_output_shapes
:??????????
?
(input_layer/DOLocationID_indicator/ShapeShape&input_layer/DOLocationID_indicator/Sum*
T0*
_output_shapes
::??
?
6input_layer/DOLocationID_indicator/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: 
?
8input_layer/DOLocationID_indicator/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:
?
8input_layer/DOLocationID_indicator/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:
?
0input_layer/DOLocationID_indicator/strided_sliceStridedSlice(input_layer/DOLocationID_indicator/Shape6input_layer/DOLocationID_indicator/strided_slice/stack8input_layer/DOLocationID_indicator/strided_slice/stack_18input_layer/DOLocationID_indicator/strided_slice/stack_2*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask
u
2input_layer/DOLocationID_indicator/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value
B :?
?
0input_layer/DOLocationID_indicator/Reshape/shapePack0input_layer/DOLocationID_indicator/strided_slice2input_layer/DOLocationID_indicator/Reshape/shape/1*
N*
T0*
_output_shapes
:
?
*input_layer/DOLocationID_indicator/ReshapeReshape&input_layer/DOLocationID_indicator/Sum0input_layer/DOLocationID_indicator/Reshape/shape*
T0*(
_output_shapes
:??????????
|
1input_layer/PULocationID_indicator/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
?????????
?
-input_layer/PULocationID_indicator/ExpandDims
ExpandDims
SelectV2_11input_layer/PULocationID_indicator/ExpandDims/dim*
T0*'
_output_shapes
:?????????
?
Ainput_layer/PULocationID_indicator/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB B 
?
;input_layer/PULocationID_indicator/to_sparse_input/NotEqualNotEqual-input_layer/PULocationID_indicator/ExpandDimsAinput_layer/PULocationID_indicator/to_sparse_input/ignore_value/x*
T0*'
_output_shapes
:?????????
?
:input_layer/PULocationID_indicator/to_sparse_input/indicesWhere;input_layer/PULocationID_indicator/to_sparse_input/NotEqual*'
_output_shapes
:?????????
?
9input_layer/PULocationID_indicator/to_sparse_input/valuesGatherNd-input_layer/PULocationID_indicator/ExpandDims:input_layer/PULocationID_indicator/to_sparse_input/indices*
Tindices0	*
Tparams0*#
_output_shapes
:?????????
?
>input_layer/PULocationID_indicator/to_sparse_input/dense_shapeShape-input_layer/PULocationID_indicator/ExpandDims*
T0*
_output_shapes
:*
out_type0	:??
?
Linput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/asset_pathConst"/device:CPU:**
_output_shapes
: *
dtype0*?
value?B? B?/cns/jd-d/home/cloud-dataengine-materialize-tempstore/serving-materialize/serving.shard-mals/1/1131/ttl=12h/c86656b7e65654a6/assets/PULocationID.txt
?
Ginput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/ConstConst*
_output_shapes
: *
dtype0	*
valueB	 R
?????????
?
Linput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/hash_tableHashTableV2*
_output_shapes
: *
	key_dtype0*?
shared_name??hash_table_/cns/jd-d/home/cloud-dataengine-materialize-tempstore/serving-materialize/serving.shard-mals/1/1131/ttl=12h/c86656b7e65654a6/assets/PULocationID.txt_263_-2_-1*
value_dtype0	
?
jinput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2InitializeTableFromTextFileV2Linput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/hash_tableLinput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/asset_path*&
 _has_manual_control_dependencies(*
	key_index?????????*
value_index?????????*

vocab_size?
?
Finput_layer/PULocationID_indicator/hash_table_Lookup/LookupTableFindV2LookupTableFindV2Linput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/hash_table9input_layer/PULocationID_indicator/to_sparse_input/valuesGinput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/Const*	
Tin0*

Tout0	*#
_output_shapes
:?????????
?
>input_layer/PULocationID_indicator/SparseToDense/default_valueConst*
_output_shapes
: *
dtype0	*
valueB	 R
?????????
?
0input_layer/PULocationID_indicator/SparseToDenseSparseToDense:input_layer/PULocationID_indicator/to_sparse_input/indices>input_layer/PULocationID_indicator/to_sparse_input/dense_shapeFinput_layer/PULocationID_indicator/hash_table_Lookup/LookupTableFindV2>input_layer/PULocationID_indicator/SparseToDense/default_value*
T0	*
Tindices0	*'
_output_shapes
:?????????
u
0input_layer/PULocationID_indicator/one_hot/ConstConst*
_output_shapes
: *
dtype0*
valueB
 *  ??
w
2input_layer/PULocationID_indicator/one_hot/Const_1Const*
_output_shapes
: *
dtype0*
valueB
 *    
s
0input_layer/PULocationID_indicator/one_hot/depthConst*
_output_shapes
: *
dtype0*
value
B :?
?
*input_layer/PULocationID_indicator/one_hotOneHot0input_layer/PULocationID_indicator/SparseToDense0input_layer/PULocationID_indicator/one_hot/depth0input_layer/PULocationID_indicator/one_hot/Const2input_layer/PULocationID_indicator/one_hot/Const_1*
T0*,
_output_shapes
:??????????
?
8input_layer/PULocationID_indicator/Sum/reduction_indicesConst*
_output_shapes
:*
dtype0*
valueB:
?????????
?
&input_layer/PULocationID_indicator/SumSum*input_layer/PULocationID_indicator/one_hot8input_layer/PULocationID_indicator/Sum/reduction_indices*
T0*(
_output_shapes
:??????????
?
(input_layer/PULocationID_indicator/ShapeShape&input_layer/PULocationID_indicator/Sum*
T0*
_output_shapes
::??
?
6input_layer/PULocationID_indicator/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: 
?
8input_layer/PULocationID_indicator/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:
?
8input_layer/PULocationID_indicator/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:
?
0input_layer/PULocationID_indicator/strided_sliceStridedSlice(input_layer/PULocationID_indicator/Shape6input_layer/PULocationID_indicator/strided_slice/stack8input_layer/PULocationID_indicator/strided_slice/stack_18input_layer/PULocationID_indicator/strided_slice/stack_2*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask
u
2input_layer/PULocationID_indicator/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value
B :?
?
0input_layer/PULocationID_indicator/Reshape/shapePack0input_layer/PULocationID_indicator/strided_slice2input_layer/PULocationID_indicator/Reshape/shape/1*
N*
T0*
_output_shapes
:
?
*input_layer/PULocationID_indicator/ReshapeReshape&input_layer/PULocationID_indicator/Sum0input_layer/PULocationID_indicator/Reshape/shape*
T0*(
_output_shapes
:??????????
q
&input_layer/fare_amount/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
?????????
?
"input_layer/fare_amount/ExpandDims
ExpandDimsfare_amount&input_layer/fare_amount/ExpandDims/dim*
T0*'
_output_shapes
:?????????
?
input_layer/fare_amount/CastCast"input_layer/fare_amount/ExpandDims*

DstT0*

SrcT0*'
_output_shapes
:?????????
w
input_layer/fare_amount/ShapeShapeinput_layer/fare_amount/Cast*
T0*
_output_shapes
::??
u
+input_layer/fare_amount/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: 
w
-input_layer/fare_amount/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:
w
-input_layer/fare_amount/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:
?
%input_layer/fare_amount/strided_sliceStridedSliceinput_layer/fare_amount/Shape+input_layer/fare_amount/strided_slice/stack-input_layer/fare_amount/strided_slice/stack_1-input_layer/fare_amount/strided_slice/stack_2*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask
i
'input_layer/fare_amount/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
?
%input_layer/fare_amount/Reshape/shapePack%input_layer/fare_amount/strided_slice'input_layer/fare_amount/Reshape/shape/1*
N*
T0*
_output_shapes
:
?
input_layer/fare_amount/ReshapeReshapeinput_layer/fare_amount/Cast%input_layer/fare_amount/Reshape/shape*
T0*'
_output_shapes
:?????????
u
*input_layer/passenger_count/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
?????????
?
&input_layer/passenger_count/ExpandDims
ExpandDimspassenger_count*input_layer/passenger_count/ExpandDims/dim*
T0*'
_output_shapes
:?????????
?
 input_layer/passenger_count/CastCast&input_layer/passenger_count/ExpandDims*

DstT0*

SrcT0*'
_output_shapes
:?????????

!input_layer/passenger_count/ShapeShape input_layer/passenger_count/Cast*
T0*
_output_shapes
::??
y
/input_layer/passenger_count/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: 
{
1input_layer/passenger_count/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:
{
1input_layer/passenger_count/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:
?
)input_layer/passenger_count/strided_sliceStridedSlice!input_layer/passenger_count/Shape/input_layer/passenger_count/strided_slice/stack1input_layer/passenger_count/strided_slice/stack_11input_layer/passenger_count/strided_slice/stack_2*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask
m
+input_layer/passenger_count/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
?
)input_layer/passenger_count/Reshape/shapePack)input_layer/passenger_count/strided_slice+input_layer/passenger_count/Reshape/shape/1*
N*
T0*
_output_shapes
:
?
#input_layer/passenger_count/ReshapeReshape input_layer/passenger_count/Cast)input_layer/passenger_count/Reshape/shape*
T0*'
_output_shapes
:?????????
|
1input_layer/payment_type_indicator/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
?????????
?
-input_layer/payment_type_indicator/ExpandDims
ExpandDims
SelectV2_21input_layer/payment_type_indicator/ExpandDims/dim*
T0*'
_output_shapes
:?????????
?
Ainput_layer/payment_type_indicator/to_sparse_input/ignore_value/xConst*
_output_shapes
: *
dtype0*
valueB B 
?
;input_layer/payment_type_indicator/to_sparse_input/NotEqualNotEqual-input_layer/payment_type_indicator/ExpandDimsAinput_layer/payment_type_indicator/to_sparse_input/ignore_value/x*
T0*'
_output_shapes
:?????????
?
:input_layer/payment_type_indicator/to_sparse_input/indicesWhere;input_layer/payment_type_indicator/to_sparse_input/NotEqual*'
_output_shapes
:?????????
?
9input_layer/payment_type_indicator/to_sparse_input/valuesGatherNd-input_layer/payment_type_indicator/ExpandDims:input_layer/payment_type_indicator/to_sparse_input/indices*
Tindices0	*
Tparams0*#
_output_shapes
:?????????
?
>input_layer/payment_type_indicator/to_sparse_input/dense_shapeShape-input_layer/payment_type_indicator/ExpandDims*
T0*
_output_shapes
:*
out_type0	:??
?
Linput_layer/payment_type_indicator/payment_type_lookup/hash_table/asset_pathConst"/device:CPU:**
_output_shapes
: *
dtype0*?
value?B? B?/cns/jd-d/home/cloud-dataengine-materialize-tempstore/serving-materialize/serving.shard-mals/1/1131/ttl=12h/c86656b7e65654a6/assets/payment_type.txt
?
Ginput_layer/payment_type_indicator/payment_type_lookup/hash_table/ConstConst*
_output_shapes
: *
dtype0	*
valueB	 R
?????????
?
Linput_layer/payment_type_indicator/payment_type_lookup/hash_table/hash_tableHashTableV2*
_output_shapes
: *
	key_dtype0*?
shared_name??hash_table_/cns/jd-d/home/cloud-dataengine-materialize-tempstore/serving-materialize/serving.shard-mals/1/1131/ttl=12h/c86656b7e65654a6/assets/payment_type.txt_4_-2_-1*
value_dtype0	
?
jinput_layer/payment_type_indicator/payment_type_lookup/hash_table/table_init/InitializeTableFromTextFileV2InitializeTableFromTextFileV2Linput_layer/payment_type_indicator/payment_type_lookup/hash_table/hash_tableLinput_layer/payment_type_indicator/payment_type_lookup/hash_table/asset_path*&
 _has_manual_control_dependencies(*
	key_index?????????*
value_index?????????*

vocab_size
?
Finput_layer/payment_type_indicator/hash_table_Lookup/LookupTableFindV2LookupTableFindV2Linput_layer/payment_type_indicator/payment_type_lookup/hash_table/hash_table9input_layer/payment_type_indicator/to_sparse_input/valuesGinput_layer/payment_type_indicator/payment_type_lookup/hash_table/Const*	
Tin0*

Tout0	*#
_output_shapes
:?????????
?
>input_layer/payment_type_indicator/SparseToDense/default_valueConst*
_output_shapes
: *
dtype0	*
valueB	 R
?????????
?
0input_layer/payment_type_indicator/SparseToDenseSparseToDense:input_layer/payment_type_indicator/to_sparse_input/indices>input_layer/payment_type_indicator/to_sparse_input/dense_shapeFinput_layer/payment_type_indicator/hash_table_Lookup/LookupTableFindV2>input_layer/payment_type_indicator/SparseToDense/default_value*
T0	*
Tindices0	*'
_output_shapes
:?????????
u
0input_layer/payment_type_indicator/one_hot/ConstConst*
_output_shapes
: *
dtype0*
valueB
 *  ??
w
2input_layer/payment_type_indicator/one_hot/Const_1Const*
_output_shapes
: *
dtype0*
valueB
 *    
r
0input_layer/payment_type_indicator/one_hot/depthConst*
_output_shapes
: *
dtype0*
value	B :
?
*input_layer/payment_type_indicator/one_hotOneHot0input_layer/payment_type_indicator/SparseToDense0input_layer/payment_type_indicator/one_hot/depth0input_layer/payment_type_indicator/one_hot/Const2input_layer/payment_type_indicator/one_hot/Const_1*
T0*+
_output_shapes
:?????????
?
8input_layer/payment_type_indicator/Sum/reduction_indicesConst*
_output_shapes
:*
dtype0*
valueB:
?????????
?
&input_layer/payment_type_indicator/SumSum*input_layer/payment_type_indicator/one_hot8input_layer/payment_type_indicator/Sum/reduction_indices*
T0*'
_output_shapes
:?????????
?
(input_layer/payment_type_indicator/ShapeShape&input_layer/payment_type_indicator/Sum*
T0*
_output_shapes
::??
?
6input_layer/payment_type_indicator/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: 
?
8input_layer/payment_type_indicator/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:
?
8input_layer/payment_type_indicator/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:
?
0input_layer/payment_type_indicator/strided_sliceStridedSlice(input_layer/payment_type_indicator/Shape6input_layer/payment_type_indicator/strided_slice/stack8input_layer/payment_type_indicator/strided_slice/stack_18input_layer/payment_type_indicator/strided_slice/stack_2*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask
t
2input_layer/payment_type_indicator/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
?
0input_layer/payment_type_indicator/Reshape/shapePack0input_layer/payment_type_indicator/strided_slice2input_layer/payment_type_indicator/Reshape/shape/1*
N*
T0*
_output_shapes
:
?
*input_layer/payment_type_indicator/ReshapeReshape&input_layer/payment_type_indicator/Sum0input_layer/payment_type_indicator/Reshape/shape*
T0*'
_output_shapes
:?????????
r
'input_layer/tolls_amount/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
?????????
?
#input_layer/tolls_amount/ExpandDims
ExpandDimstolls_amount'input_layer/tolls_amount/ExpandDims/dim*
T0*'
_output_shapes
:?????????
?
input_layer/tolls_amount/CastCast#input_layer/tolls_amount/ExpandDims*

DstT0*

SrcT0*'
_output_shapes
:?????????
y
input_layer/tolls_amount/ShapeShapeinput_layer/tolls_amount/Cast*
T0*
_output_shapes
::??
v
,input_layer/tolls_amount/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: 
x
.input_layer/tolls_amount/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:
x
.input_layer/tolls_amount/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:
?
&input_layer/tolls_amount/strided_sliceStridedSliceinput_layer/tolls_amount/Shape,input_layer/tolls_amount/strided_slice/stack.input_layer/tolls_amount/strided_slice/stack_1.input_layer/tolls_amount/strided_slice/stack_2*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask
j
(input_layer/tolls_amount/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
?
&input_layer/tolls_amount/Reshape/shapePack&input_layer/tolls_amount/strided_slice(input_layer/tolls_amount/Reshape/shape/1*
N*
T0*
_output_shapes
:
?
 input_layer/tolls_amount/ReshapeReshapeinput_layer/tolls_amount/Cast&input_layer/tolls_amount/Reshape/shape*
T0*'
_output_shapes
:?????????
s
(input_layer/trip_distance/ExpandDims/dimConst*
_output_shapes
: *
dtype0*
valueB :
?????????
?
$input_layer/trip_distance/ExpandDims
ExpandDimstrip_distance(input_layer/trip_distance/ExpandDims/dim*
T0*'
_output_shapes
:?????????
?
input_layer/trip_distance/CastCast$input_layer/trip_distance/ExpandDims*

DstT0*

SrcT0*'
_output_shapes
:?????????
{
input_layer/trip_distance/ShapeShapeinput_layer/trip_distance/Cast*
T0*
_output_shapes
::??
w
-input_layer/trip_distance/strided_slice/stackConst*
_output_shapes
:*
dtype0*
valueB: 
y
/input_layer/trip_distance/strided_slice/stack_1Const*
_output_shapes
:*
dtype0*
valueB:
y
/input_layer/trip_distance/strided_slice/stack_2Const*
_output_shapes
:*
dtype0*
valueB:
?
'input_layer/trip_distance/strided_sliceStridedSliceinput_layer/trip_distance/Shape-input_layer/trip_distance/strided_slice/stack/input_layer/trip_distance/strided_slice/stack_1/input_layer/trip_distance/strided_slice/stack_2*
Index0*
T0*
_output_shapes
: *
shrink_axis_mask
k
)input_layer/trip_distance/Reshape/shape/1Const*
_output_shapes
: *
dtype0*
value	B :
?
'input_layer/trip_distance/Reshape/shapePack'input_layer/trip_distance/strided_slice)input_layer/trip_distance/Reshape/shape/1*
N*
T0*
_output_shapes
:
?
!input_layer/trip_distance/ReshapeReshapeinput_layer/trip_distance/Cast'input_layer/trip_distance/Reshape/shape*
T0*'
_output_shapes
:?????????
Y
input_layer/concat/axisConst*
_output_shapes
: *
dtype0*
value	B :
?
input_layer/concatConcatV2*input_layer/DOLocationID_indicator/Reshape*input_layer/PULocationID_indicator/Reshapeinput_layer/fare_amount/Reshape#input_layer/passenger_count/Reshape*input_layer/payment_type_indicator/Reshape input_layer/tolls_amount/Reshape!input_layer/trip_distance/Reshapeinput_layer/concat/axis*
N*
T0*(
_output_shapes
:??????????
?"
Const_3Const*
_output_shapes
:	?*
dtype0*?!
value?!B?!	?"?!??Z M@	??kL@?O???J@<%?OM@!B?MFM@?@Y?G?L@??S?&L@w?S?q?L@?vټ?L@]???M@???'?/M@?M?HRL@4????LL@??Ո7L@
(?x?tK@%?ہ ?L@?y???K@@-$<??L@h??u?fK@??-?o?L@??y7??N@?F
z??K@?0??.L@t?H2[hL@?????M@>~Qf)N@|9#?!M@~i/???L@?n???;M@,?+???L@?vC?$M@??!?BL@Xkw?-`J@~#??%M@?F4(?+M@??M@??tP?/M@̑??AqL@?+???M@??J\?M@????o?L@?.?P?L@??3?AwL@v??o@L@??M?``L@c???\7L@CUvz'M@??+?M@??M@???XL@R	BA8L@?0???K@?(?bL@?Ό??SL@|ʂ?H?K@?ms?L@/)?>?M@?;???K@???-M@LC0?L@?ML@????%M@ ׫AL@x?1???M@?#?p0L@c?M-m?K@??ި?4M@????!L@6=}?]?L@???gGM@?Lt?oL@Ƥ?K@??5?a'M@%?_??M@cr??E?L@J? /dL@?:Vy?M@.=i??M@??(??L@gD??yBM@?F?
}VL@?N??gvJ@?*?5?K@#???#L@?????K@??V۱K@?l??PM@mP?z??M@I?h_0:M@??i??TL@???ψL@???B?K@???`%L@FǠ?#M@?:b@t?L@???0EM@??wTL@??C8-M@.???#M@:ބ??L@??- nM@`^??`K@???r?K@?SD?UM@.?/gL@??n??4M@r?????L@J??z?gM@&????6M@?p????L@??v.?	M@?{4!L@$8???+L@???o??K@G?p??M@?Pj?C|M@???
:)M@t??*`?L@>QfZ? M@??g??'M@?"??nHM@!1N?sBL@#???TL@???0:L@??????J@_?(ԯ'M@??[u?"M@?;??+M@y!tu? M@?!?7L@h0?mL@J[R3?(M@???v0M@{???%M@??i???L@jO???K@?2M?J@}???JL@
7??*?K@ ?????K@?oA?,M@? L@?????L@?v?T?sL@ ???K@BR???K@{??L@????5L@N????
L@p??\?M@?[??M@n8?Ƙ^L@Ub?BL@=c?"?M@w-??$?M@????rMM@??K4?K@& <{?K@
6?u?L@?>?[+L@???t'&M@???ܗxL@????UM@0#|??M@X??xtM@?tV$}K@?0?Q@RM@??`?L@[?)d?L@??`t?uL@~J?L@????%M@?Lr?M@????2M@k??ZL@.[??|L@s2??L@??fi#M@n?Mh9"M@?Vg-L@?yu yM@=?S?)M@X??V?J@L???{L@?L????L@& ??KL@&6?-F{M@'???M@m"?T?<M@?;?M@?oMtL@??Vf'?L@'?C?xK@???M?M@???mo M@???\M@H?͙?K@= ]?Q}L@?rgh?!M@l?K??L@jVV??K@?Ӓ
??L@|?:@?L@*Q???M@?6??,L@?????M@??\?'M@oU??VM@Ջ?PEL@F;?Z?$M@|??2KM@@?+YL@?????L@"?5؄M@????7M@?z???L@??Eg*K@?rP'UL@/A?/?L@?)o;??L@??M??L@?(?G)?K@t3-?K@byZ??'L@Ñ?xM@So?ۣ#M@??4???L@?g???!M@?oU?K@??? cNL@?????bL@u???M@?"?5dL@?9??L@NߔSf?K@w??\O?K@?	P2?L@RrNL@?????L@??۽?]L@J6?6L@?k??9L@b8?:"M@/#???RM@x?_3b?K@??T??8L@??S?L@??~???L@??{?mL@q???pM@?4??M@?dFF?xL@??c??#M@?yy|lM@??ʥ??K@+,???K@??<4oL@V?h?!M@n&?rM@5D??chM@?K?(V,M@?|??#M@????p\@?:?g?t\@?ɥ??\@?4?[@Rf)*?[@K?m??Z@??9)?[@?tY8\@Su5??\@d?9?eo\@?]??v\@?s?y??[@J?2??[@*̔?[@?D? ?E\@k?L-2\\@/?p?*?[@7K?j x\@?QY?n\@-83!\@y??8
\@n՘n??[@g?rw??\@?C?WuD\@??3?N\@??j?t?[@??#???[@?vl7WR\@?I?<??\@??H??\@??/?;+\@?u ?7\@?*?lY\@?w?E?\@7JD??r\@??? r\@??ђ?a\@?.]8v\@??*?.\@?t$x?[@?j?%#\@??ڥ??[@???p\@P??|ĳ[@&??~?o[@?7??xo\@.?7Ax\@#~E?6P\@??b%.\@?a`??[@??3??[@i??b"\@"???W|\@~??=?v\@??q0?n\@?V???h\@L?ec??[@~k?ɩ\@??3S?S\@??7??[@??D?
?[@|{? x}\@1??.Rl\@x?ܬq!\@+(?s?\@??ۮ??[@)? ,Y\@w炟{\@??}??[@&:?#?8\@?~?m/?[@??<4??[@????W\@#?ЅM6\@If???w\@?b??o\@?(?&\@z]ӄ -\@?,?/\@Ҵy?
0\@/:}?e\@³͗?\@jS??v\@?(?&?[@???M?T\@`Ck?[@ ??_?[@g8{\\@_???,\@s~?G?r\@c??Z?\@?VRO?[@1??\@8<??[@z ????[@???I?w\@_???y\@??????[@&?@??[@_?`??U\@????2\@h??Z?[@??]?Oy\@?#??w\@??g?W\@????Mt\@?~???v\@?-??ys\@??[?F\@???ׂ~\@Ry?!?v\@?????[@????[@t??ߚ[@|'*1]@?fT)?x\@???^\@???-?[@.????V\@?kp|\@??N???[@???[?i\@u(?d??[@A??Цu\@?f???\@?^6?nv\@???[@&?go\@??v?2?[@??qu\@?))??s\@?1??s\@1q~X??[@??^r?\@j??: \@? C+??[@`?J?u\@kQ?r\@??i?l\@?K?	hp\@2{t?\@?q???\@??Ѣ?[@??I{?\@????[@b?h(a\@?ђ?m\@9;???^\@?).B??[@?C???[@`?N}E?[@p?q??[@Ġ?b!?[@??wX?h\@"??DӾ[@???#P?[@`?	??[@7??3t\@c???Z?\@???q\@	>?"e?[@??$ȑ?[@N???s\@?VO#t\@??????[@uM??U?[@Gq?߰\@ ݖ?H\@ј$؁l\@:??ݗ?[@X@?nb?[@"F"?\@??qVf\@?!???\@?????]\@4
?)K?[@*S??\@?Ys}?[@?8???[@Ə????\@;'.?\@l?+ ? \@Մkd?^\@??<?l/\@ἴKY\@!z?lrk\@5????[@??9w?\@?]\@N^???
\@+.{??o\@?碋?q\@ՙ?8,\@??r?i\@??֏??[@E?	??\@?<|rn?[@?(E~?[@??ݣ??\@61?DYp\@????N\@??????\@ᇎ?a\@|???r\@??\??f\@??? ??[@ã?(?[@?'?w?[@?}=K??[@ķD?"v\@?e?m?[@(gQ_X?[@
	?{??[@H˶̰\@y??L?s\@X????\@n$?z\@sa	??[@?0??[@??i?%s\@?,?>?v\@J?CV?\@??ur[@?9?9`\@?-B?Y?[@?-??+7\@???ffb\@?QʆW}\@dk?%"k\@r?E?A?[@?e}?H\@?s29ڦZ@Aof??v\@ b}l?`\@ I?=a?[@?.G\@ؑ?O\@?~ī}y\@N???[p\@"iY?]\@??-{?\@<&;z.?[@??twpy\@j??50x\@???v\@?B]???[@JT؃v\@Ě螹d\@?(???\@,?????[@?ū8_\@?Ľt\@Hiߵi\@a?ާ-\@Ǭ??\@??c??[@?X p\@?B#Z5v\@WSu?a\@??s?w\@\pJ??[@h<???[@z/?s[@?c???h?????LW????><?H@@?hc?bvA@A?&t=@@?5???@@]ӈ?Aa??n??X&b??
?
weightsVarHandleOp*
_class
loc:@weights*
_output_shapes
: *
dtype0*
shape:	?*
shared_name	weights
_
(weights/IsInitialized/VarIsInitializedOpVarIsInitializedOpweights*
_output_shapes
: 
i
weights/AssignAssignVariableOpweightsConst_3*&
 _has_manual_control_dependencies(*
dtype0
d
weights/Read/ReadVariableOpReadVariableOpweights*
_output_shapes
:	?*
dtype0
P
Const_4Const*
_output_shapes
: *
dtype0*
valueB 2??&Yt?i?
?
	interceptVarHandleOp*
_class
loc:@intercept*
_output_shapes
: *
dtype0*
shape: *
shared_name	intercept
c
*intercept/IsInitialized/VarIsInitializedOpVarIsInitializedOp	intercept*
_output_shapes
: 
m
intercept/AssignAssignVariableOp	interceptConst_4*&
 _has_manual_control_dependencies(*
dtype0
_
intercept/Read/ReadVariableOpReadVariableOp	intercept*
_output_shapes
: *
dtype0
b
CastCastinput_layer/concat*

DstT0*

SrcT0*(
_output_shapes
:??????????
^
MatMul/ReadVariableOpReadVariableOpweights*
_output_shapes
:	?*
dtype0
_
MatMulMatMulCastMatMul/ReadVariableOp*
T0*'
_output_shapes
:?????????
X
compute/ReadVariableOpReadVariableOp	intercept*
_output_shapes
: *
dtype0
b
computeAddV2MatMulcompute/ReadVariableOp*
T0*'
_output_shapes
:?????????
X
initNoOp^intercept/Assign^weights/Assign*&
 _has_manual_control_dependencies(
?
init_all_tablesNoOpk^input_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2k^input_layer/PULocationID_indicator/PULocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2k^input_layer/payment_type_indicator/payment_type_lookup/hash_table/table_init/InitializeTableFromTextFileV2*&
 _has_manual_control_dependencies(
+

group_depsNoOp^init^init_all_tables
?
init_all_tables_1NoOpk^input_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2k^input_layer/PULocationID_indicator/PULocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2k^input_layer/payment_type_indicator/payment_type_lookup/hash_table/table_init/InitializeTableFromTextFileV2
Y
save/filename/inputConst*
_output_shapes
: *
dtype0*
valueB Bmodel
n
save/filenamePlaceholderWithDefaultsave/filename/input*
_output_shapes
: *
dtype0*
shape: 
e

save/ConstPlaceholderWithDefaultsave/filename*
_output_shapes
: *
dtype0*
shape: 
{
save/StaticRegexFullMatchStaticRegexFullMatch
save/Const"/device:CPU:**
_output_shapes
: *
pattern
^s3://.*
a
save/Const_1Const"/device:CPU:**
_output_shapes
: *
dtype0*
valueB B.part
f
save/Const_2Const"/device:CPU:**
_output_shapes
: *
dtype0*
valueB B
_temp/part
|
save/SelectSelectsave/StaticRegexFullMatchsave/Const_1save/Const_2"/device:CPU:**
T0*
_output_shapes
: 
f
save/StringJoin
StringJoin
save/Constsave/Select"/device:CPU:**
N*
_output_shapes
: 
Q
save/num_shardsConst*
_output_shapes
: *
dtype0*
value	B :
k
save/ShardedFilename/shardConst"/device:CPU:0*
_output_shapes
: *
dtype0*
value	B : 
?
save/ShardedFilenameShardedFilenamesave/StringJoinsave/ShardedFilename/shardsave/num_shards"/device:CPU:0*
_output_shapes
: 
?
save/SaveV2/tensor_namesConst"/device:CPU:0*
_output_shapes
:*
dtype0*'
valueBB	interceptBweights
v
save/SaveV2/shape_and_slicesConst"/device:CPU:0*
_output_shapes
:*
dtype0*
valueBB B 
?
save/SaveV2SaveV2save/ShardedFilenamesave/SaveV2/tensor_namessave/SaveV2/shape_and_slicesintercept/Read/ReadVariableOpweights/Read/ReadVariableOp"/device:CPU:0*&
 _has_manual_control_dependencies(*
dtypes
2
?
save/control_dependencyIdentitysave/ShardedFilename^save/SaveV2"/device:CPU:0*
T0*'
_class
loc:@save/ShardedFilename*&
 _has_manual_control_dependencies(*
_output_shapes
: 
?
+save/MergeV2Checkpoints/checkpoint_prefixesPacksave/ShardedFilename^save/control_dependency"/device:CPU:0*
N*
T0*
_output_shapes
:
?
save/MergeV2CheckpointsMergeV2Checkpoints+save/MergeV2Checkpoints/checkpoint_prefixes
save/Const"/device:CPU:0*&
 _has_manual_control_dependencies(
?
save/IdentityIdentity
save/Const^save/MergeV2Checkpoints^save/control_dependency"/device:CPU:0*
T0*
_output_shapes
: 
?
save/RestoreV2/tensor_namesConst"/device:CPU:0*
_output_shapes
:*
dtype0*'
valueBB	interceptBweights
y
save/RestoreV2/shape_and_slicesConst"/device:CPU:0*
_output_shapes
:*
dtype0*
valueBB B 
?
save/RestoreV2	RestoreV2
save/Constsave/RestoreV2/tensor_namessave/RestoreV2/shape_and_slices"/device:CPU:0*
_output_shapes

::*
dtypes
2
N
save/Identity_1Identitysave/RestoreV2*
T0*
_output_shapes
:
z
save/AssignVariableOpAssignVariableOp	interceptsave/Identity_1*&
 _has_manual_control_dependencies(*
dtype0
P
save/Identity_2Identitysave/RestoreV2:1*
T0*
_output_shapes
:
z
save/AssignVariableOp_1AssignVariableOpweightssave/Identity_2*&
 _has_manual_control_dependencies(*
dtype0
t
save/restore_shardNoOp^save/AssignVariableOp^save/AssignVariableOp_1*&
 _has_manual_control_dependencies(
-
save/restore_allNoOp^save/restore_shard"?
<
save/Const:0save/Identity:0save/restore_all (5 @F8"?
asset_filepaths?
?
Ninput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/asset_path:0
Ninput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/asset_path:0
Ninput_layer/payment_type_indicator/payment_type_lookup/hash_table/asset_path:0"?
saved_model_assets?*?
?
+type.googleapis.com/tensorflow.AssetFileDefd
P
Ninput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/asset_path:0DOLocationID.txt
?
+type.googleapis.com/tensorflow.AssetFileDefd
P
Ninput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/asset_path:0PULocationID.txt
?
+type.googleapis.com/tensorflow.AssetFileDefd
P
Ninput_layer/payment_type_indicator/payment_type_lookup/hash_table/asset_path:0payment_type.txt",
saved_model_main_op

init_all_tables_1"?
table_initializer?
?
jinput_layer/DOLocationID_indicator/DOLocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2
jinput_layer/PULocationID_indicator/PULocationID_lookup/hash_table/table_init/InitializeTableFromTextFileV2
jinput_layer/payment_type_indicator/payment_type_lookup/hash_table/table_init/InitializeTableFromTextFileV2"?
trainable_variables??
I
	weights:0weights/Assignweights/Read/ReadVariableOp:0(2	Const_3:08
O
intercept:0intercept/Assignintercept/Read/ReadVariableOp:0(2	Const_4:08"?
	variables??
I
	weights:0weights/Assignweights/Read/ReadVariableOp:0(2	Const_3:08
O
intercept:0intercept/Assignintercept/Read/ReadVariableOp:0(2	Const_4:08*?
serving_default?
1
DOLocationID!
DOLocationID:0?????????
1
PULocationID!
PULocationID:0?????????
/
fare_amount 
fare_amount:0?????????
7
passenger_count$
passenger_count:0?????????
1
payment_type!
payment_type:0?????????
1
tolls_amount!
tolls_amount:0?????????
3
trip_distance"
trip_distance:0?????????8
predicted_tip_amount 
	compute:0?????????tensorflow/serving/predict