require 'mongo'
require 'aws-sdk'
require 'rack/utils'

class Gridfs2S3

  def initialize db, s3, prefix='public/'
    raise ArgumentError unless db.is_a?(Mongo::Database)
    raise ArgumentError unless s3.is_a?(Aws::S3::Bucket)
    raise ArgumentError unless prefix.is_a?(String)
    @db = db
    @s3 = s3
    @prefix = prefix
  end

  def copy_file req
    file = find_file req
    return false if file.nil?
    copy file
    return true
  end

  def copy_all_files
    @db.fs.find.each do |f|
      copy f
    end
  end

  private

  def copy file
    @db.fs.open_download_stream(file['_id']) do |stream|
      @s3.put_object({
        acl: 'public-read',
        key: "#{@prefix}#{file['filename']}",
        content_type: file['contentType'],
        body: stream.read,
        metadata: {
          parent_collection: file['metadata']['parent_collection'],
        }
      })
    end
  end

  # This comes from rack-grid-serve and is fully tested
  
  def id_or_filename str
    if BSON::ObjectId.legal? str
      BSON::ObjectId.from_string str
    else
      Rack::Utils.unescape str
    end
  end
  
  def find_file req
    str = id_or_filename req
    if str.is_a? BSON::ObjectId
      @db.fs.find({_id: str}).first
    else
      @db.fs.find({
        '$or' => [
          {filename: str},
          {filename: "/#{str}"}
        ]
      }).first
    end
  end

end

