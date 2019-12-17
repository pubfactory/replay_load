require "replay_load/version"
require "replay_load/cli"
require "replay_load/session"

module ReplayLoad
	def self.es_client
		es_client = Elasticsearch::Client.new log: true, url: 'https://414ca18c7fee433fa6435c04003a31d0.eu-west-1.aws.found.io:9243', user: 'elastic', password: 'GvTNfeFfq62rwEsz5hfD9fpW'
		es_client.transport.logger.level = Logger::WARN
		es_client
	end
end
