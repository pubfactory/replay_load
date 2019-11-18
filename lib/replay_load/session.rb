require 'jmeter'
require 'nokogiri'
require 'date'

module ReplayLoad
	class Session
		attr_reader :jsessionid, :index, :requests
		def initialize(jsessionid:,index:)
			@jsessionid = jsessionid
			@index = index
			@requests = []
		end
		def populate
			response = ReplayLoad.es_client.search(
				index: @index,
				body: {
					size: 1000,
					query: {
						bool: {
							must: [
								{ term: { 'session.jsessionid.keyword' => @jsessionid } },
							],
							#must_not: [
							#	{ regexp: { 'request.uri.keyword' => '.*\\.(png|jpg|gif|ico|css|js)' } },
							#],
						},
					},
					sort: [
						{ '@timestamp' => 'asc' },
						{ offset: 'asc' },
					],
				}
			)

			response['hits']['hits'].each do |hit|
				@requests << hit
			end
		end
		def to_jmx
			fragment = Nokogiri::XML::DocumentFragment.new Jmeter::DOCUMENT
			hashtree = Nokogiri::XML::Element.new 'hashTree', Jmeter::DOCUMENT
			container = Nokogiri::XML::Element.new 'GenericController', Jmeter::DOCUMENT
			container[:guiclass] = 'LogicControllerGui'
			container[:testclass] = 'GenericController'
			container[:testname] = @jsessionid
			container[:enabled] = true
			fragment.add_child container
			fragment.add_child hashtree
		
			@requests.each do |request|
				sample = Jmeter::HttpSampler.new
				sample.path = request['_source']['request']['uri']
				sample[:testname] = request['_source']['request']['uri']
				sample.method = request['_source']['request']['method']
				sample.ip_source = request['_source']['request']['ip'];
				# CHECKED HERE
				sample[:timestamp] = request['_source']['@timestamp'];
				hashtree.add_child sample
				hashtree.add_child Nokogiri::XML::Element.new 'hashTree', Jmeter::DOCUMENT
			end
			fragment
		end

		def to_csv
			fragment = ""
			@requests.each do |request|
				initial_timestamp = request['_source']['@timestamp'];
				datetime = DateTime.strptime(initial_timestamp,'%Y-%m-%dT%H:%M:%S.000Z')
				unix_ts = datetime.to_time.to_i
				fragment << "https://qa.dgsg-web.qa.sites.pubfactory.com"
				fragment << request['_source']['request']['uri']
				fragment << ","
				fragment <<  request['_source']['request']['method']
				fragment << ","
				fragment <<  request['_source']['request']['ip'];
				fragment << ","
				fragment <<  unix_ts.to_s
				#fragment <<  request['_source']['@timestamp'];
				fragment << ","
				fragment << @jsessionid
				fragment << "\n"
			end
			fragment
		end
	end
end
