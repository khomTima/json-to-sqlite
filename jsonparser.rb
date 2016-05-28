#!/usq/bin/env ruby
require 'json'
require 'sqlite3'

data = JSON.parse(File.open(ARGV[0]).read)

tableName = ARGV[1]

# Table creation
fields = Hash.new
$index = 0
dataArray = data["metadata"]["fields"]
puts "dataArray = #{dataArray}"
puts "dataArray.count = #{dataArray.count}"
while $index < dataArray.count do
	type = dataArray[$index]["type"];
	if ( type == 'String') 
		type = 'STR'
	end
	name = dataArray[$index]["name"];
	fields[name] = type
	$index += 1
end

puts "fields = #{fields}"

db = SQLite3::Database.open( "test.db" )

mappedString = fields.map { |key, value| "#{key} #{value}" }.join(', ')
puts "mappedString = #{mappedString}"
db.execute("CREATE TABLE IF NOT EXISTS #{tableName} (#{mappedString})")

# Table filling

rowDataArray = Array.new
$index = 0
rowDataSourceArray = data["rowdata"]["rows"]
while $index < rowDataSourceArray.count do
	rowHash = Hash.new
	keys = fields.keys
	$keyIndex = 0
	while $keyIndex < keys.count
		key = keys[$keyIndex]
		rowHash[key] = "'#{rowDataSourceArray[$index][key]}'"
		$keyIndex += 1
	end
	rowDataArray[$index] = rowHash
	$index += 1
end

$index = 0
while $index < rowDataArray.count do
	puts "rowDataItem = #{rowDataArray[$index]}"
	rowDataItem = rowDataArray[$index]
	mappedRowKeysString = rowDataItem.map { |key, value| "#{key}" }.join(', ')
	mappedRowValuesString = rowDataItem.map { |key, value| "#{value}" }.join(', ')
	puts "mappedRowKeysString = #{mappedRowKeysString}"
	puts "mappedRowValuesString = #{mappedRowValuesString}"
	db.execute("INSERT INTO #{tableName} (#{mappedRowKeysString}) VALUES(#{mappedRowValuesString})")
	$index += 1
end

rows = db.execute("select * from #{tableName}")
puts rows
