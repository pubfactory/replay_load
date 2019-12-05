require 'thor'
require 'elasticsearch'
require 'json'

module ReplayLoad
	class CLI < Thor
		def self.exit_on_failure?; true; end
		
		@@start_date = '2019-03-01T12:00:00.000Z'
		@@end_date   = '2019-03-01T12:20:00.000Z'

		desc 'health', 'get cluster health'			
		def health
			puts ReplayLoad.es_client.cluster.health
		end

		desc 'get_all_sessions', 'Get all sessions'
		def get_all_sessions
			response = ReplayLoad.es_client.search(
				size: 10000,
				body: {
					"query": {
      						"bool": {
        						"must": [
          							{ "range": { "@timestamp" => {
                  							"gte": @@start_date,
                  							"lte": @@end_date
                							}
            							   }	
       							 	},
								{
      									"terms": {
           									"_index" => ["pubfactory-requests-2019.03.01"] 
        								}
      								},
        							{ "exists": {"field" => "session.jsessionid"}}
        						],
        						"must_not": [{
            							"regexp": { 
              								"request.uri.keyword" => '.*\\.(png|jpg|gif|ico|css|js)'
              							} 
        						}]
      						}
    					}				
				}
			)
			sessions = []
			#print response['hits']['total']
			response['hits']['hits'].each do |hit|
					fragment = ""
					initial_timestamp = hit['_source']['@timestamp'];
                                	datetime = DateTime.strptime(initial_timestamp,'%Y-%m-%dT%H:%M:%S.000Z')
                                	# Multiply by 1000 as Ruby converts timestamp to unix time in seconds
                                	unix_ts = datetime.to_time.to_i * 1000
                                	fragment << "https://dg.dgsg-web.qa.sites.pubfactory.com"
                                	fragment << hit['_source']['request']['uri']
                                	fragment << ","
                                	fragment <<  hit['_source']['request']['method']
                                	fragment << ","
                                	fragment <<  hit['_source']['request']['ip'];
                               		fragment << ","
                                	fragment <<  unix_ts.to_s
                                	fragment << ","
                                	fragment <<  hit['_source']['@timestamp'];
                                	fragment << ","
                                	fragment << hit['_source']['session']['jsessionid']
                                	fragment << "\n"
                        
	                       		print fragment
			end
			#response['aggregations']['sessions']['buckets'].sort_by{|bucket| bucket['key']}.each do |bucket|
			#	sessions << Session.new(jsessionid: bucket['key'], index: 'pubfactory-requests-2019.03.01')
			#end

			#sessions[4].populate
			#puts sessions[4].to_jmx
			#return

#			sessions.map do |session|
#				session.populate 
#			end
#			sessions.reject! do |session|
#				session.requests.length == 0
#			end

#			sessions.each do |session|
#				puts session.to_jmx
#				puts session.to_csv
#			end
		end

		desc 'update_sessions', 'Fix broken sessions'
		def update_sessions
			response = ReplayLoad.es_client.search(
				index: 'pubfactory-requests-2019.03.01',
				body: {
					query: {
						range: {
							'@timestamp' => {
								gte: @@start_date,
								lte: @@end_date,
							},
						},
					},
					aggs: {
						sessions: {
							terms: {
								field: 'session.jsessionid.keyword',
								size: 100000,
							},
						},
					},
				}
			)
			response['aggregations']['sessions']['buckets'].sort_by{|bucket| bucket['key']}.each do |bucket|
				response2 = ReplayLoad.es_client.search(
					index: 'pubfactory-requests-*',
					body: {
						size: 1,
						query: {
							term: {
								'session.jsessionid.keyword' => bucket['key'],
							},
						},
						sort: [
							{
								'@timestamp' => {
									order: 'asc'
								}
							}
						]
					}
				)

				# Go get the first request from the session due to a bug in log format
				next if response2['hits']['hits'][0]['_source']['request']['referrer'].nil?
				initial_referrer = response2['hits']['hits'][0]['_source']['request']['referrer'].sub('https://www.degruyter.com','')
				initial_timestamp = response2['hits']['hits'][0]['_source']['@timestamp']
				response3 = ReplayLoad.es_client.search(
					index: 'pubfactory-requests-*',
					body: {
						size: 500,
						query: {
							bool: {
								must: [
									{
										term: {
											'request.uri.keyword' => initial_referrer
										}
									},
									{
										range: {
											'@timestamp' => {
												gte: (Time.parse(initial_timestamp) - 5).iso8601(3),
												lte: initial_timestamp,
											},
										},
									},
								],
							},
						},
					}
				)

				if response3['hits']['hits'].length == 1
					unless response3['hits']['hits'][0]['_source']['session'].nil?
						if response3['hits']['hits'][0]['_source']['session']['jsessionid'].nil?
							ReplayLoad.es_client.update(
								index: response3['hits']['hits'][0]['_index'],
								id: response3['hits']['hits'][0]['_id'],
								type: response3['hits']['hits'][0]['_type'],
								body: {
									doc: {
										session: {
											jsessionid: response2['hits']['hits'][0]['_source']['session']['jsessionid'],
										},
									},
								},
							)
						end
					end
				end
			end
		end
	end
end
