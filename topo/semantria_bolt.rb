require 'semantria'

class SemantriaBolt < RedStorm::DSL::Bolt
  output_fields :string

  class SessionCallbackHandler < CallbackHandler
    def onRequest(sender, args)
      puts "Request: ", args
    end

    def onResponse(sender, args)
      puts "Response: ", args
    end

    def onError(sender, args)
      puts 'Error: ', args
    end

    def onDocsAutoResponse(sender, args)
      puts "DocsAutoResponse: ", args.length, args
    end

    def onCollsAutoResponse(sender, args)
      puts "CollsAutoResponse: ", args.length, args
    end
  end

  on_init do
    @queued_tuples = { }
    create_session
    @poller = detach_semantria_poller
  end

  on_close do
    # Don't actually know if/how session should be closed
    #@session.close if @session
  end

  on_receive :emit => false do |tuple|
    # Queue document to the Semantria service
    # Poll will later get the result to emit
    message = tuple[0].to_s
    queue_document(message)
    ack(tuple)
  end


  private
  
  def create_session
    consumer_key = 'f43bdb95-b836-4ed8-ba83-bd4875fa19c2'
    consumer_secret = '849d1522-6d1b-411c-86f5-13199d744adf'

    # Initializes new session with the keys and app name.
    # We also will use compression.
    session = Session.new(consumer_key, consumer_secret, 'TestApp', true)
    # Initialize session callback handlers
    callback = SessionCallbackHandler.new()
    session.setCallbackHandler(callback)
    @session = session
  end

  def queue_document(message)
    id = rand(10 ** 10).to_s.rjust(10, '0')
    doc = {'id' => id, 'text' => message}
    @queued_tuples[id] = message
    status = @session.queueDocument(doc)
    # Check status from Semantria service
    if status == 202
      log.info 'Document ' + doc['id'] + ' queued successfully.'
    end
  end


  def detach_semantria_poller
    Thread.new do
      Thread.current.abort_on_exception = true

      loop do
        sleep(1)
        # Requests processed results from Semantria service
        status = @session.getProcessedDocuments()
        # Check status from Semantria service
        status.is_a? Array and status.each do |data|
          id = data['id']
          score = data['sentiment_score']
          message = @queued_tuples[id]
          if message
            unanchored_emit(message, score)
            @queued_tuples.delete(id) # Clean up tuple
          end
        end
        puts status.length, ' documents received successfully.'
      end
    end

  end
end
