module Resque
  # The stat subsystem. Used to keep track of integer counts.
  #
  #   Get a stat:  Stat[name]
  #   Incr a stat: Stat.incr(name)
  #   Decr a stat: Stat.decr(name)
  #   Kill a stat: Stat.clear(name)
  module Stat
    extend self
    extend Helpers

    # Returns the int value of a stat, given a string stat name.
    def get(stat)
      res = mongo_stats.respond_to?(:find_one) ? mongo_stats.find_one(:stat => stat) : mongo_stats.find(:stat => stat).first
      return 0 unless res
      res['value'].to_i
    end

    # Alias of `get`
    def [](stat)
      get(stat)
    end

    # For a string stat name, increments the stat by one.
    #
    # Can optionally accept a second int parameter. The stat is then
    # incremented by that amount.
    def incr(stat, by = 1)
      if mongo_stats.respond_to?(:update)
        mongo_stats.update({:stat => stat}, {'$inc' => {:value => by}}, :upsert => true)
      else
        mongo_stats.find(:stat => stat).update({'$inc' => {:value => by}}, :upsert => true)
      end
    end

    # Increments a stat by one.
    def <<(stat)
      incr stat
    end

    # For a string stat name, decrements the stat by one.
    #
    # Can optionally accept a second int parameter. The stat is then
    # decremented by that amount.
    def decr(stat, by = 1)
      if mongo_stats.respond_to?(:update)
        mongo_stats.update({:stat => stat}, {'$inc' => {:value => -by}}, :upsert => true)
      else
        mongo_stats.find(:stat => stat).update({'$inc' => {:value => -by}}, :upsert => true)
      end
    end

    # Decrements a stat by one.
    def >>(stat)
      decr stat
    end

    # Removes a stat from Redis, effectively setting it to 0.
    def clear(stat)
      if mongo_stats.respond_to?(:remove)
        mongo_stats.remove(:stat => stat)
      else
        mongo_stats.find(:stat => stat).remove
      end
    end
  end
end
