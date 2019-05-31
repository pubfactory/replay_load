require 'thor'
require 'elasticsearch'
require 'json'

module ReplayLoad
	class CLI < Thor
		def self.exit_on_failure?; true; end
		
		@@start_date = '2019-03-01T12:00:00.000Z'
		@@end_date   = '2019-03-01T12:01:00.000Z'

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
			puts <<~EOF
				<?xml version="1.0" encoding="UTF-8"?>
				<jmeterTestPlan version="1.2" properties="4.0" jmeter="4.0 r1823414">
					<hashTree>
						<TestPlan guiclass="TestPlanGui" testclass="TestPlan" testname="Test Plan" enabled="true">
							<stringProp name="TestPlan.comments"/>
							<boolProp name="TestPlan.functional_mode">false</boolProp>
							<boolProp name="TestPlan.tearDown_on_shutdown">true</boolProp>
							<boolProp name="TestPlan.serialize_threadgroups">false</boolProp>
							<elementProp name="TestPlan.user_defined_variables" elementType="Arguments" guiclass="ArgumentsPanel" testclass="Arguments" testname="User Defined Variables" enabled="true">
								<collectionProp name="Arguments.arguments"/>
							</elementProp>
							<stringProp name="TestPlan.user_define_classpath"/>
						</TestPlan>
						<hashTree>
							<ThreadGroup guiclass="ThreadGroupGui" testclass="ThreadGroup" testname="Thread Group" enabled="true">
								<stringProp name="ThreadGroup.on_sample_error">continue</stringProp>
								<elementProp name="ThreadGroup.main_controller" elementType="LoopController" guiclass="LoopControlPanel" testclass="LoopController" testname="Loop Controller" enabled="true">
									<boolProp name="LoopController.continue_forever">true</boolProp>
								</elementProp>
								<stringProp name="ThreadGroup.num_threads">100</stringProp>
								<stringProp name="ThreadGroup.ramp_time">300</stringProp>
								<boolProp name="ThreadGroup.scheduler">false</boolProp>
								<stringProp name="ThreadGroup.duration">900</stringProp>
								<stringProp name="ThreadGroup.delay"></stringProp>
							</ThreadGroup>
							<hashTree>
								<HeaderManager guiclass="HeaderPanel" testclass="HeaderManager" testname="HTTP Header Manager" enabled="true">
									<collectionProp name="HeaderManager.headers"/>
								</HeaderManager>
								<hashTree/>
								<CookieManager guiclass="CookiePanel" testclass="CookieManager" testname="HTTP Cookie Manager" enabled="true">
									<collectionProp name="CookieManager.cookies"/>
									<boolProp name="CookieManager.clearEachIteration">true</boolProp>
								</CookieManager>
								<hashTree/>
								<CacheManager guiclass="CacheManagerGui" testclass="CacheManager" testname="HTTP Cache Manager" enabled="true">
									<boolProp name="clearEachIteration">true</boolProp>
									<boolProp name="useExpires">true</boolProp>
								</CacheManager>
								<hashTree/>
								<ConfigTestElement guiclass="HttpDefaultsGui" testclass="ConfigTestElement" testname="HTTP Request Defaults" enabled="true">
									<elementProp name="HTTPsampler.Arguments" elementType="Arguments" guiclass="HTTPArgumentsPanel" testclass="Arguments" testname="User Defined Variables" enabled="true">
										<collectionProp name="Arguments.arguments"/>
									</elementProp>
									<stringProp name="HTTPSampler.domain">www.degruyter.com</stringProp>
									<stringProp name="HTTPSampler.port">443</stringProp>
									<stringProp name="HTTPSampler.protocol">https</stringProp>
									<stringProp name="HTTPSampler.contentEncoding"></stringProp>
									<stringProp name="HTTPSampler.path"></stringProp>
									<boolProp name="HTTPSampler.image_parser">true</boolProp>
									<boolProp name="HTTPSampler.concurrentDwn">true</boolProp>
									<stringProp name="HTTPSampler.concurrentPool">10</stringProp>
									<stringProp name="HTTPSampler.embedded_url_re">https://www\.degruyter\.com/.*</stringProp>
									<stringProp name="HTTPSampler.connect_timeout"></stringProp>
									<stringProp name="HTTPSampler.response_timeout"></stringProp>
								</ConfigTestElement>
								<hashTree/>
								<UniformRandomTimer guiclass="UniformRandomTimerGui" testclass="UniformRandomTimer" testname="Random Session Start Delay" enabled="true">
									<stringProp name="ConstantTimer.delay">0</stringProp>
									<stringProp name="RandomTimer.range">5000.0</stringProp>
								</UniformRandomTimer>
								<hashTree/>
								<RandomController guiclass="RandomControlGui" testclass="RandomController" testname="Random Session Selector" enabled="true">
									<intProp name="InterleaveControl.style">1</intProp>
								</RandomController>
								<hashTree>
			EOF
			sessions.each do |session|
				puts session.to_jmx
			end
			puts <<~EOF
								</hashTree>
							</hashTree>
						</hashTree>
					</hashTree>
				</jmeterTestPlan>
			EOF
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
