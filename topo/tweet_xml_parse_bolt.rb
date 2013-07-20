require 'nokogiri'
#require 'json'

class TweetXmlParseBolt < RedStorm::DSL::Bolt
  on_receive :emit => true, :ack => true, :anchor => true do |tuple|
    tweet = {}
    doc = Nokogiri::XML(tuple[0].to_s)
    
    # extract desired fields
    id = doc.at_css("id")
    author = doc.at_css("author name")
    author_uri = doc.at_css("author uri")
    summary = doc.at_css("summary")
    
    # add desired fields with values to hash
    tweet[:id] = id.content unless id.nil?
    tweet[:author] = author.content unless author.nil?
    tweet[:author_uri] = author_uri.content unless author_uri.nil?
    tweet[:summary] = summary.content unless summary.nil?
    
    [tweet[:id], tweet[:author], tweet[:author_uri], tweet[:summary]]
#    [tweet.to_json]  #unless tweet.empty?
  end
end

