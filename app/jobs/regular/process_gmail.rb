require 'google/apis/gmail_v1'
require 'googleauth'

module Jobs
  class ProcessGmail < Jobs::Scheduled
    sidekiq_options retry: false

    APPLICATION_NAME = "Discourse Sync Service"

    GMAIL_CLIENT_ID = "332222246267-js8j5mm5kebahonp6teohejqeala85fl.apps.googleusercontent.com"
    GMAIL_CLIENT_SECRET = "WmE4AMik3bmS2JjIwWNQ2VDH"
    GMAIL_REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"
    GMAIL_REFRESH_TOKEN = "gmail_authorization"

    def execute(args)
      @args = args || {}

      group = Group.find_by(email_username: args[:email_address])
      if !group
        Rails.logger.warn("No group was found for email address: #{args[:email_address]}.")
        return
      end

      credentials = Google::Auth::UserRefreshCredentials.new(
        client_id: GMAIL_CLIENT_ID,
        client_secret: GMAIL_CLIENT_SECRET,
        scope: Google::Apis::GmailV1::AUTH_SCOPE,
        redirect_uri: GMAIL_REDIRECT_URI,
        refresh_token: group.custom_fields[GMAIL_REFRESH_TOKEN]
      )
      credentials.fetch_access_token!

      service = Google::Apis::GmailV1::GmailService.new
      service.client_options.application_name = APPLICATION_NAME
      service.authorization = credentials

      page_token = nil
      loop do
        list = service.list_user_histories(args[:email_address], start_history_id: args[:history_id], page_token: page_token)

        list.history.each do |history|
          history.messages.each do |message|
            begin
              message = service.get_user_message(args[:email_address], message.id, format: 'raw')

              email = {
                "UID" => message.id,
                "FLAGS" => [],
                "LABELS" => message.label_ids,
                "RFC822" => message.raw,
              }

              receiver = Email::Receiver.new(email["RFC822"],
                destinations: [{ type: :group, obj: group }],
                uid_validity: args[:history_id],
                uid: -1
              )
              receiver.process!

              Imap::Sync.update_topic(email, receiver.incoming_email)
            rescue Email::Receiver::ProcessingError => e
            end
          end
        end

        page_token = list.next_page_token
        break if page_token == nil
      end

      nil
    end
  end
end
