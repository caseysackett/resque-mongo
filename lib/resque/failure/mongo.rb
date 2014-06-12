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
          if Resque.mongo_failures.respond_to?(:insert)
            Resque.mongo_failures.insert(data)
          else
            Resque.mongo_failures << data
          end
        rescue Exception => e
          data[:payload] = self.class.process_paypload(payload)
          if Resque.mongo_failures.respond_to?(:insert)
            Resque.mongo_failures.insert(data)
          else
            Resque.mongo_failures << data
          end
        end
      end

      def self.process_paypload(p)
        p.to_s
      end

      def self.count
        if Resque.mongo_failures.respond_to?(:count)
          Resque.mongo_failures.count
        else
          Resque.mongo_failures.find.count
        end
      end

      def self.all(start = 0, count = 1)
        start, count = [start, count].map { |n| Integer(n) }
        all_failures = Resque.mongo_failures.find().sort([:$natural, :desc]).skip(start).limit(count).to_a
        all_failures.size == 1 ? all_failures.first : all_failures
      end

      def self.clear
        if Resque.mongo_failures.respond_to?(:remove)
          Resque.mongo_failures.remove
        else
          Resque.mongo_failures.find.remove
        end
      end

      def self.requeue(index)
        item = all(index)
        item['retried_at'] = Time.now.strftime("%Y/%m/%d %H:%M:%S")
        if Resque.mongo_failures.respond_to?(:update)
          Resque.mongo_failures.update({:_id => item['_id']}, item)
        else
          Resque.mongo_failures.find(:_id => item['_id']).update(item)
        end
        Job.create(item['queue'], item['payload']['class'], *item['payload']['args'])
      end

      def self.remove(index)
        item = all(index)
        if Resque.mongo_failures.respond_to?(:remove)
          Resque.mongo_failures.remove({:_id => item['_id']})
        else
          Resque.mongo_failures.find(:_id => item['_id']).remove
        end
      end
    end
  end
end
