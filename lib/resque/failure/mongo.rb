module Resque
  module Failure
    # A Failure backend that stores exceptions in Mongo. Very simple but
    # works out of the box, along with support in the Resque web app.
    class Mongo < Base
      def save
        data = {
          :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S"),
          :payload   => payload,
          :exception => exception.class.to_s,
          :error     => exception.to_s,
          :backtrace => Array(exception.backtrace),
          :worker    => worker.to_s,
          :queue     => queue
        }
        begin
          Resque.mongo_failures.insert_one(data)
        rescue Exception => e
          data[:payload] = self.class.process_paypload(payload)
          Resque.mongo_failures.insert_one(data)
        end
      end

      def self.process_paypload(p)
        p.to_s
      end

      def self.count
        Resque.mongo_failures.find.count
      end

      def self.all(start = 0, count = 1)
        start, count = [start, count].map { |n| Integer(n) }
        all_failures = Resque.mongo_failures.find().sort([:$natural, :desc]).skip(start).limit(count).to_a
        all_failures.size == 1 ? all_failures.first : all_failures
      end

      def self.clear
        Resque.mongo_failures.delete_many
      end

      def self.requeue(index)
        item = all(index)
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        Resque.mongo_failures.find(:_id => item['_id']).replace_one(:retried_at => item['retried_at'])
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end

      def self.remove(index)
        item = all(index)
        Resque.mongo_failures.find(:_id => item['_id']).delete_one
      end
    end
  end
end
