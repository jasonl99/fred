module Fred
  class Client
    class TooManyRequestsError < StandardError; end
    class BadRequestError < StandardError; end
    class NotFoundError < StandardError; end
    class LockedError < StandardError; end
    class InternalServerError < StandardError; end

    RETRY_RESPONSE_CODES = [423, 429]  # Locked,Too Many Requests

    include HTTParty
    base_uri "https://api.stlouisfed.org/fred"
    format :xml
        
    attr_reader :api_key
                
    def initialize(options={})
      @api_key = options[:api_key] || Fred.api_key
    end

    def category(secondary, options={})
      if secondary.nil?
        mashup(self.get("/category", :query => options.merge(self.default_options)))
      else
        mashup(self.get("/category/#{secondary}", :query => options.merge(self.default_options)))			
      end
    end
    
    def releases(secondary, options={})
      if secondary.nil?
        mashup(self.get("/releases", :query => options.merge(self.default_options)))
      else
        mashup(self.get("/releases/#{secondary}", :query => options.merge(self.default_options)))			
      end
    end
    
    def release(secondary, options={})
      if secondary.nil?
        mashup(self.get("/release", :query => options.merge(self.default_options)))
      else
        mashup(self.get("/release/#{secondary}", :query => options.merge(self.default_options)))			
      end
    end
    
    def series(secondary, options={})
      if secondary.nil?
        mashup(self.get("/series", :query => options.merge(self.default_options)))
      else
        mashup(self.get("/series/#{secondary}", :query => options.merge(self.default_options)))			
      end
    end
    
    def sources(secondary, options={})
      if secondary.nil?
        mashup(self.get("/sources?", :query => options.merge(self.default_options)))
      else
        mashup(self.get("/sources/#{secondary}", :query => options.merge(self.default_options)))			
      end
    end
    
    def source(secondary, options={})
      if secondary.nil?
        mashup(self.get("/source", :query => options.merge(self.default_options)))
      else
        mashup(self.get("/source/#{secondary}", :query => options.merge(self.default_options)))			
      end
    end

    def backoff_errors
      @backoff_errors ||= []
    end
    
    protected
    
    def default_options
      {:api_key => @api_key}
    end
    # Errors caught (and raised) here with specific codes are from the FRED API
    # documentation at https://fred.stlouisfed.org/docs/api/fred/errors.html
    # All non-200 responses will cause an error to be raised.
    def mashup(response)
      error = self.error_text(response) unless response.code == 200

      case response.code
      when 200
        if response.is_a?(Hash)
          Hashie::Mash.new(response)
        else
          if response.first.is_a?(Hash)
            response.map{|item| Hashie::Mash.new(item)}
          else
            response
          end
        end
      when 400
        raise BadRequestError.new(error)
      when 404
        raise NotFoundError.new(error)
      when 423
        raise LockedError.new(error)
      when 429
        raise TooManyRequestsError.new(error)
      when 500
        raise InternalServerError.new(error)
      else
        raise "Error: Unhandled response code #{response.code} - #{error}"
      end
    end

    def error_text(response)
      error_text = response.parsed_response.dig("error", "message") if response.parsed_response&.is_a?(Hash)
      error_text ||= response.message
    end

    # TODO make the retry variables configurable
    def get(url, **options)
      max_attempts = 5
      base_backoff = 15
      backoff_exponent = 2
      current_attempts = 0
      response = nil

      while current_attempts < max_attempts
        response = self.class.get(url, options)
        break unless response.code.in?(RETRY_RESPONSE_CODES)

        sleep_time = base_backoff * (backoff_exponent ** current_attempts)

        self.backoff_errors << {
          url: url,
          time: DateTime.now,
          response_code: response.code,
          error_text: error_text(response),
          headers: response.headers,
          backoff_time: sleep_time
        }

        current_attempts += 1
        sleep(sleep_time)
      end
      
      response
    end


  end
end
