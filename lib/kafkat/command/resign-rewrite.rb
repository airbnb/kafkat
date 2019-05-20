module Kafkat
  module Command
    class ResignForce < Base
      register_as 'resign-rewrite'

      usage 'resign-rewrite <broker id>',
            'Forcibly rewrite leaderships to exclude a broker.'

      usage 'resign-rewrite <broker id> --force',
            'Same as above but proceed if there are no available ISRs.'

      def run
        broker_id = ARGV[0] && ARGV.shift.to_i
        if broker_id.nil?
          puts "You must specify a broker ID."
          exit 1
        end

        opts = Optimist.options do
          opt :force, "force"
        end

        print "This operation rewrites leaderships in ZK to exclude broker '#{broker_id}'.\n"
        print "WARNING: This is a last resort. Try the 'shutdown' command first!\n\n".red

        return unless agree("Proceed (y/n)?")

        brokers = zookeeper.get_brokers
        topics = zookeeper.get_topics
        force = opts[:force]

        ops = {}
        topics.each do |_, t|
          t.partitions.each do |p|
            next if p.leader != broker_id

            alternates = p.isr.reject { |i| i == broker_id }
            new_leader_id = alternates.sample

            if !new_leader_id && !force
              print "Partition #{t.name}-#{p.id} has no other ISRs!\n"
              exit 1
            end

            new_leader_id ||= -1
            ops[p] = new_leader_id
          end
        end

        print "\n"
        print "Summary of the new assignments:\n\n"

        print "Partition\tLeader\n"
        ops.each do |p, lid|
          print justify("#{p.topic_name}-#{p.id}")
          print justify(lid.to_s)
          print "\n"
        end

        begin
          print "\nStarting.\n"
          ops.each do |p, lid|
            retryable(tries: 3, on: Interface::Zookeeper::WriteConflictError) do
              zookeeper.write_leader(p, lid)
            end
          end
        rescue Interface::Zookeeper::WriteConflictError => e
          print "Failed to update leaderships in ZK. Try re-running.\n\n"
          exit 1
        end

        print "Done.\n"
      end
    end
  end
end
