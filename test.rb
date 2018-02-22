require 'minitest/autorun'
require 'gridfs2s3'

require 'mongo'

Mongo::Logger.logger.level = Logger::ERROR
mongo = Mongo::Client.new([ '127.0.0.1:27017' ], :database => 'gridfs2s3-test')
$db = mongo.database
$db.drop

require 'aws-sdk'

s3_resource = Aws::S3::Resource.new
$s3 = s3_resource.bucket(ENV['BUCKET'])

class TestGridfs2S3 < Minitest::Test

  # parallelize_me!

  def create_test_fs_file txt
    file = Tempfile.new('foo')
    file.write(txt)
    file.rewind
    $db.fs.upload_from_stream(
      "text-file/#{txt}.txt",
      file, {
        content_type: 'text/plain',
        metadata: {
          parent_collection: 'text-file',
        }
      }
    )
    file.close
    file.unlink
  end

  def setup
    $db.drop
    create_test_fs_file 'hello'
    create_test_fs_file 'world'
    $s3.clear!
  end

  def teardown
    $db.drop
    $s3.clear!
  end

  def test_raises_when_arguments_are_wrong_type
    assert_raises ArgumentError do 
      Gridfs2S3.new '', $s3
    end
    assert_raises ArgumentError do 
      Gridfs2S3.new $db, ''
    end
    assert_raises ArgumentError do 
      Gridfs2S3.new $db, $s3, {}
    end
  end

  def test_returns_false_if_file_doesnt_exist
    fs2s3 = Gridfs2S3.new $db, $s3
    refute fs2s3.copy_file 'text-file/unknown.txt'
  end

  def assert_file_copied txt
    s3file = $s3.object("public/text-file/#{txt}.txt")
    assert s3file.exists?
    assert_equal 'text/plain', s3file.content_type
    assert_equal 'text-file', s3file.metadata['parent_collection']
    assert_equal txt, s3file.get.body.read
  end

  def test_copy_file
    fs2s3 = Gridfs2S3.new $db, $s3
    assert fs2s3.copy_file("text-file/hello.txt")
    assert_file_copied 'hello'
    refute $s3.object('public/text-file/world.txt').exists?
  end

  def test_copy_all_files
    fs2s3 = Gridfs2S3.new $db, $s3
    fs2s3.copy_all_files
    assert_file_copied 'hello'
    assert_file_copied 'world'
  end

end

