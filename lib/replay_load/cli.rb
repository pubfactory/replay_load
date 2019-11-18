require 'thor'
require 'elasticsearch'
require 'json'
require 'time'

module ReplayLoad
	class CLI < Thor
		def self.exit_on_failure?; true; end
		
		@@start_date = '2019-03-01T12:00:00.000Z'
		@@end_date   = '2019-03-01T12:00:30.000Z'

		desc 'health', 'get cluster health'			
		def health
			puts ReplayLoad.es_client.cluster.health
		end

		desc 'dump_sessions', 'Dump a list of sessions'
		def dump_sessions
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
				puts bucket['key']
			end
		end
		
		desc 'dump_lines', 'Dump log lines to stdout'
		def dump_lines
			response = ReplayLoad.es_client.search(
				index: 'pubfactory-requests-2019.04.22',
				scroll: '1m',
				body: {
					size: 500,
				}
			)
			scroll_id = response['_scroll_id']
			response['hits']['hits'].each do |hit|
				puts hit['_source']['message']
			end
			until response['hits']['hits'].empty? do
				response = es.scroll(
					body: {
						scroll_id: scroll_id,
						scroll: '1m',
					}
				)
				response['hits']['hits'].each do |hit|
					puts hit['_source']['message']
				end
				scroll_id = response['_scroll_id']
			end
		end
		
		desc 'get_session', 'Get a specific session'
		def get_session
			response = ReplayLoad.es_client.search(
				index: 'pubfactory-requests-*',
				body: {
					size: 500,
					query: {
						term: {
							'session.jsessionid.keyword' => 'C04E8D2433CB8D71CD0FCEE792C7FFCA',
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

			response['hits']['hits'].each do |hit|
				puts hit['_source'].to_json
			end
		end

		desc 'get_all_sessions', 'Get all sessions'
		def get_all_sessions
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
			sessions = []
			response['aggregations']['sessions']['buckets'].sort_by{|bucket| bucket['key']}.each do |bucket|
				sessions << Session.new(jsessionid: bucket['key'], index: 'pubfactory-requests-2019.03.01')
			end

			#sessions[4].populate
			#puts sessions[4].to_jmx
			#return

			sessions.map do |session|
				session.populate 
			end
			sessions.reject! do |session|
				session.requests.length == 0
			end

			# Field names defined at https://jmeter.apache.org/api/constant-values.html#org.apache.jmeter.protocol.http.sampler.HTTPSamplerBase
			sessions.each do |session|
				puts session.to_csv
			end
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
