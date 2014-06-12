require 'mongo'

require 'resque/version'

require 'resque/errors'

require 'resque/failure'
require 'resque/failure/base'

require 'resque/helpers'
require 'resque/stat'
require 'resque/job'
require 'resque/worker'
require 'resque/plugin'
require 'resque/queue'

module Resque
  include Helpers
  extend self

  # Accepts a 'hostname:port' string or a Redis server.
  def mongo=(server)
    case server
    when Moped::Session
      @con = server
      @db = @con
      @mongo = @con['monque']
      @workers = @db['workers']
      @failures = @db['failures']
      @stats = @db['stats']
    when Mongo::Client
      @con = server
      @con.use("monque")
      @db = @con
      @mongo = @db['monque']
      @workers = @db['workers']
      @failures = @db['failures']
      @stats = @db['stats']
    when Hash
      host, port = server['host'], server['port']
      @con = Mongo::Connection.new(host, port)
      if server['username'] && server['password']
        @con.add_auth('monque', server['username'], server['password'])
        @con.apply_saved_authentication
      end
      @db = @con.db('monque')
      @mongo = @db.collection('monque')
      @workers = @db.collection('workers')
      @failures = @db.collection('failures')
      @stats = @db.collection('stats')
      add_indexes
    when String
      host, port = server.split(':')
      @con = Mongo::Connection.new(host, port)
      @db = @con.db('monque')
      @mongo = @db.collection('monque')
      @workers = @db.collection('workers')
      @failures = @db.collection('failures')
      @stats = @db.collection('stats')
      add_indexes
    end
  end


  # Returns the current Redis connection. If none has been created, will
  # create a new one.
  def mongo
    return @mongo if @mongo
    self.mongo = 'localhost:27017'
    self.mongo
  end

  def mongo_workers
    return @workers if @workers
    self.mongo = 'localhost:27017'
    @workers
  end

  def mongo_failures
    return @failures if @failures
    self.mongo = 'localhost:27017'
    @failures
  end

  def mongo_stats
    return @stats if @stats
    self.mongo = 'localhost:27017'
    @stats
  end
  
  # The `before_first_fork` hook will be run in the **parent** process
  # only once, before forking to run the first job. Be careful- any
  # changes you make will be permanent for the lifespan of the
  # worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_first_fork(&block)
    block ? (@before_first_fork = block) : @before_first_fork
  end

  # Set a proc that will be called in the parent process before the
  # worker forks for the first time.
  attr_writer :before_first_fork

  # The `before_fork` hook will be run in the **parent** process
  # before every job, so be careful- any changes you make will be
  # permanent for the lifespan of the worker.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def before_fork(&block)
    block ? (@before_fork = block) : @before_fork
  end

  # Set the before_fork proc.
  attr_writer :before_fork

  # The `after_fork` hook will be run in the child process and is passed
  # the current job. Any changes you make, therefore, will only live as
  # long as the job currently being processed.
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def after_fork(&block)
    block ? (@after_fork = block) : @after_fork
  end

  # Set the after_fork proc.
  attr_writer :after_fork

  def to_s
    if @con.respond_to?(:host)
      "Mongo Client connected to #{@con.host}"
    else
      "Mongo Client connected to #{@con.inspect.gsub('<','').gsub('>','').html_safe}"
    end
  end

  attr_accessor :inline

  # If 'inline' is true Resque will call #perform method inline
  # without queuing it into Redis and without any Resque callbacks.
  # The 'inline' is false Resque jobs will be put in queue regularly.
  alias :inline? :inline

  def add_indexes
    @mongo.create_index :queue if @mongo.respond_to?(:create_index)
    @workers.create_index :worker if @workers.respond_to?(:create_index)
    @stats.create_index :stat if @stats.respond_to?(:create_index)
  end

  def drop
    @mongo.drop if @mongo
    @workers.drop if @workers
    @failures.drop if @failures
    @stats.drop if @stats
    @mongo = nil
  end
  
  #
  # queue manipulation
  #

  # Pushes a job onto a queue. Queue name should be a string and the
  # item should be any JSON-able Ruby object.
  def push(queue, item)
    if mongo.respond_to?(:insert)
      # moped
      mongo.insert({ :queue => queue.to_s, :item => encode(item) })
    else
      # mongo ruby driver
      mongo << { :queue => queue.to_s, :item => encode(item) }
    end
  end

  # Pops a job off a queue. Queue name should be a string.
  #
  # Returns a Ruby object.
  def pop(queue)
    doc = nil
    if mongo.respond_to?(:find_and_modify)
      # mongo ruby driver
      doc = mongo.find_and_modify( :query => { :queue => queue },
                                   :remove => true )
    else
      # moped
      doc = mongo.find.modify({:queue => queue}, :remove => true)
    end
    return nil if !doc
    decode doc['item']
  rescue Mongo::OperationFailure => e
    return nil if e.message =~ /No matching object/
    raise e
  end

  # Returns an integer representing the size of a queue.
  # Queue name should be a string.
  def size(queue)
    mongo.find(:queue => queue).count
  end

  # Returns an array of items currently queued. Queue name should be
  # a string.
  #
  # start and count should be integer and can be used for pagination.
  # start is the item to begin, count is how many items to return.
  #
  # To get the 3rd page of a 30 item, paginatied list one would use:
  #   Resque.peek('my_list', 59, 30)
  def peek(queue, start = 0, count = 1)
    start, count = [start, count].map { |n| Integer(n) }
    res = mongo.find(:queue => queue).skip(start).limit(count).to_a
    res.collect! { |doc| decode(doc['item']) }
    
    if count == 1
      return nil if res.empty?
      res.first
    else
      return [] if res.empty?
      res
    end
  end

  # Returns an array of all known Resque queues as strings.
  def queues
    if mongo.respond_to?(:distinct)
      # mongo ruby driver
      mongo.distinct(:queue)
    else
      # moped
      mongo.find.distinct(:queue)
    end
  end
  
  # Given a queue name, completely deletes the queue.
  def remove_queue(queue)
    if mongo.respond_to?(:remove)
      # mongo ruby driver
      mongo.remove(:queue => queue)
    else
      # moped
      mongo.find(:queue => queue).remove_all
    end
  end

  #
  # job shortcuts
  #

  # This method can be used to conveniently add a job to a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # This method is considered part of the `stable` API.
  def enqueue(klass, *args)
    Job.create(queue_from_class(klass), klass, *args)
  end

  # Just like `enqueue` but allows you to specify the queue you want to
  # use. Runs hooks.
  #
  # `queue` should be the String name of the queue you're targeting.
  #
  # Returns true if the job was queued, nil if the job was rejected by a
  # before_enqueue hook.
  #
  # This method is considered part of the `stable` API.
  def enqueue_to(queue, klass, *args)
    # Perform before_enqueue hooks. Don't perform enqueue if any hook returns false
    before_hooks = Plugin.before_enqueue_hooks(klass).collect do |hook|
      klass.send(hook, *args)
    end
    return nil if before_hooks.any? { |result| result == false }

    Job.create(queue, klass, *args)

    Plugin.after_enqueue_hooks(klass).each do |hook|
      klass.send(hook, *args)
    end

    return true
  end

  # This method can be used to conveniently remove a job from a queue.
  # It assumes the class you're passing it is a real Ruby class (not
  # a string or reference) which either:
  #
  #   a) has a @queue ivar set
  #   b) responds to `queue`
  #
  # If either of those conditions are met, it will use the value obtained
  # from performing one of the above operations to determine the queue.
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If no args are given, this method will dequeue *all* jobs matching
  # the provided class. See `Resque::Job.destroy` for more
  # information.
  #
  # Returns the number of jobs destroyed.
  #
  # Example:
  #
  #   # Removes all jobs of class `UpdateNetworkGraph`
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph)
  #
  #   # Removes all jobs of class `UpdateNetworkGraph` with matching args.
  #   Resque.dequeue(GitHub::Jobs::UpdateNetworkGraph, 'repo:135325')
  #
  # This method is considered part of the `stable` API.
  def dequeue(klass, *args)
    Job.destroy(queue_from_class(klass), klass, *args)
  end

  # Given a class, try to extrapolate an appropriate queue based on a
  # class instance variable or `queue` method.
  def queue_from_class(klass)
    klass.instance_variable_get(:@queue) ||
      (klass.respond_to?(:queue) and klass.queue)
  end

  # This method will return a `Resque::Job` object or a non-true value
  # depending on whether a job can be obtained. You should pass it the
  # precise name of a queue: case matters.
  #
  # This method is considered part of the `stable` API.
  def reserve(queue)
    Job.reserve(queue)
  end

    # Validates if the given klass could be a valid Resque job
  #
  # If no queue can be inferred this method will raise a `Resque::NoQueueError`
  #
  # If given klass is nil this method will raise a `Resque::NoClassError`
  def validate(klass, queue = nil)
    queue ||= queue_from_class(klass)

    if !queue
      raise NoQueueError.new("Jobs must be placed onto a queue.")
    end

    if klass.to_s.empty?
      raise NoClassError.new("Jobs must be given a class.")
    end
  end

  #
  # worker shortcuts
  #

  # A shortcut to Worker.all
  def workers
    Worker.all
  end

  # A shortcut to Worker.working
  def working
    Worker.working
  end

  # A shortcut to unregister_worker
  # useful for command line tool
  def remove_worker(worker_id)
    worker = Resque::Worker.find(worker_id)
    worker.unregister_worker
  end

  #
  # stats
  #

  # Returns a hash, similar to redis-rb's #info, of interesting stats.
  def info
    return {
      :pending   => queues.inject(0) { |m,k| m + size(k) },
      :processed => Stat[:processed],
      :queues    => queues.size,
      :workers   => workers.size.to_i,
      :working   => working.size,
      :failed    => Stat[:failed],
      :servers   => @con.respond_to?(:host) ? ["#{@con.host}:#{@con.port}"] : [@con.inspect.gsub('<','').gsub('>','').html_safe],
      :environment  => ENV['RAILS_ENV'] || ENV['RACK_ENV'] || 'development'
    }
  end

  # Returns an array of all known Resque keys in Redis. Redis' KEYS operation
  # is O(N) for the keyspace, so be careful - this can be slow for big databases.
  def keys
    queues
  end
end
