import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import urllib.parse

from config import config
from log_util import LoggingUtil


logger = LoggingUtil.init_logging(__name__)

class MailCanary:
    def __init__(self, smtp_server, smtp_port, sender_email, sender_password=None):
        """
        Initialize the Email Canary.
        :param smtp_server: SMTP server address.
        :param smtp_port: SMTP server port.
        :param sender_email: Sender's email address.
        :param sender_password: Sender's email password or app-specific password.
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_email = sender_email
        self.sender_password = sender_password

    def deployed_email_template(self, kg_name, version, access_url, endpoint_url=None):
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Service Deployment Successful</title>    
        </head>
        <body>
            <div class="container">
                <div class="header">Service Deployment Successful</div>
                <div class="content">                      
                    <p>We are pleased to inform you that your service has been successfully deployed 
                    and is now operational.</p>
                </div>
                <div class="details">
                    <p><strong>Service Name:</strong> {kg_name}</p>
                    <p><strong>Version:</strong> {version}</p>
                    <p><strong>Query page:</strong> <a href="{access_url}" target="_blank">FRINK Query Page</a></p>
                    {f'<p><strong>SPARQL endpoint:</strong> <code>{endpoint_url}</code><br>'
                       f'<span style="font-size:90%">Use this from a SPARQL client. Opening it in a browser with no '
                       f'query returns 404 -- that is QLever answering, not an outage.</span></p>' if endpoint_url else ''}
                </div>
                <div class="content">
                    If you have any questions or need assistance, please contact us at 
                    <a href="mailto:okn-frink@renci.org">okn-frink@renci.org</a>.
                </div>
            </div>
        </body>
        </html>
        """

    def review_email_template(self,
                              repository_url,
                              repository_name,
                              branch_name,
                              version,
                              github_pr,
                              github_branch):
        graph_char_message = f"""
        <h4>Automated Graph Characterization Updates</h4>
            <ol>
                <li>To view any changes to your automated graph characterization please follow this link: <a href="{github_branch}">
                Graph Characterization Branch</a></li>
                <li>We have also created a github pull request for your review <a href="{github_pr}">Link to Pull request</a></li>
                <li>Please review this automated characterization to ensure it aligns with your Graph.</li>
            </ol>
        """
        # disable graph char message for now 
        graph_char_message = ""
        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Review and Tag FRINK Lakefs </title>
        </head>
        <body>
            <p>Greetings, </p>
            <br>
            
            <p>You are receiving this automated message from the FRINK Landing Zone Bot to inform you about a recent 
            change to your knowledge graph.</p>
        
        
            <h4> Lakefs Reviews </h4>
            <p>
                Your most recent lakeFS upload to <a href="{repository_url}/objects">{repository_name}</a> 
                has been successfully converted to HDT.
            </p>
        
            <ul>
                <li><strong>Repository:</strong> {repository_name}</li>
                <li><strong>Branch:</strong> {branch_name}</li>
            </ul>
        
            <p>Please follow the steps below to review and tag the branch. Tagging the branch allows you to indicate 
            the different versions of your knowledge graph and to preserve each version in time.</p>            
            
        
            <ol>
                <li>Review the changes and ensure everything is in order on this branch <a href="{repository_url}/objects?ref={branch_name}">{branch_name}</a>.</li>                
        
                <li>Visit the <a href="{repository_url}/tags">Tags Page</a>.</li>
                
                <li>Click "Create Tag" button.
                <li>Please enter <b>{version}</b> as the "Tag Name" and select "{branch_name}" as "From branch". 
                <strong>We highly recommended using this version as the rest of the automation relies on this.</strong>
                </li>               
                
                <li>Submit the form by clicking "Create".</li>
            </ol>
            {graph_char_message}
            <p>When the above steps are properly completed, this version of your knowledge graph will be deployed in the 
            query servers, and you will receive another notification once the deployment is complete.</p>
            
            <p>If you have any questions or need further assistance, please don't hesitate to reach out to 
            <a href="mailto:okn-frink@renci.org">okn-frink@renci.org</a> .</p>
        
            <p>Thank you for your cooperation!</p>
        
        </body>
        </html>
        """

    def send_email(self, recipient_email, subject, body):
        """
        Send a single-part HTML email to avoid 'multipart/mixed' spam flags.
        """
        try:
            # Change: Using MIMEText directly instead of MIMEMultipart
            # This ensures the Content-Type is 'text/html' without a 'boundary'
            message = MIMEText(body, 'html', 'utf-8')
            message['From'] = self.sender_email
            message['To'] = ", ".join([x.strip() for x in recipient_email.split(',')])
            message['Subject'] = subject

            # Connect and send
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()  # Secure the connection
                if self.sender_password:
                    server.login(self.sender_email, self.sender_password)

                if config.stop_email:
                    logger.info(f"STOP_EMAIL set; not sending '{subject}' to {recipient_email}")
                else:
                    server.send_message(message)

            logger.info(f"Email sent to {recipient_email}")

        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            raise e

    def notify_event(self, recipient_email, event_name, **kwargs):
        """
        Adjusted to send HTML instead of plain text to stay consistent.
        """
        subject = f"Notification: {event_name}"
        # Wrapping in a <pre> tag so line breaks in the string are preserved in HTML
        body = f"<html><body><h3>Event: {event_name}</h3><pre>"
        for key, value in kwargs.items():
            body += f"- {key}: {value}\n"
        body += "</pre></body></html>"
        self.send_email(recipient_email, subject, body)

    def send_review_email(self,
                          recipient_email: str,
                          repository_name: str,
                          version: str,
                          branch_name: str,
                          github_pr: str,
                          github_branch: str,
                          ):
        repository_url = config.lakefs_public_url.rstrip('/') + '/repositories/' + repository_name
        logger.info(f"Sending review email to {recipient_email}")
        email_body = self.review_email_template(
            repository_url=repository_url,
            repository_name=repository_name,
            version=version,
            branch_name=branch_name,
            github_pr=github_pr,
            github_branch=github_branch
        )
        self.send_email(recipient_email, "Deployment Review Request", email_body)

    def send_deployed_email(self,
                            recipient_email: str,
                            version: str,
                            kg_name: str,
                            kg_slug: str = ""):
        """`kg_name` is the human title for the subject and body; `kg_slug` is
        the registry shortname, which is what the query page's `sources` filter
        and the endpoint path are keyed on. They are NOT interchangeable: the
        title contains spaces and parentheses and produced a malformed URL."""
        base = config.frink_address.rstrip('/')
        if not base.startswith(('http://', 'https://')):
            base = 'https://' + base
        query = ("PREFIX+rdf:+%3Chttp://www.w3.org/1999/02/22-rdf-syntax-ns%23%3E%0A"
                 "PREFIX+rdfs:+%3Chttp://www.w3.org/2000/01/rdf-schema%23%3E%0A"
                 "SELECT+*+WHERE+{%0A++?sub+?pred+?obj+.%0A}+LIMIT+10")
        slug = urllib.parse.quote(kg_slug or "", safe="")
        access_url = f"{base}/?query={query}" + (f"&sources={slug}" if slug else "")
        # /{slug}/sparql is the canonical form. The HTTPRoute rewrites the KG
        # prefix away, so /{slug}, /{slug}/ and /{slug}/sparql all reach the
        # server -- but /sparql is what clients expect, and every one of them
        # needs a ?query=: a bare GET is a 404 from QLever itself.
        endpoint_url = f"{base}/{slug}/sparql" if slug else None
        email_body = self.deployed_email_template(
            kg_name=kg_name,
            version=version,
            access_url=access_url,
            endpoint_url=endpoint_url,
        )
        self.send_email(recipient_email, f"{kg_name} {version} Deployed", email_body)


mail_canary = MailCanary(
    config.smtp_server, config.smtp_port, config.email_address, config.email_password
)

# if __name__=="__main__":
    # mail_canary = MailCanary(
    #     "mail.smtp2go.com",
    #     "2525",
    #     "no-reply@okn.us",
    #     "OpgplUSRcIU09X95"
    # )
    # mail_canary.send_deployed_email("kebedey@renci.org", "v1.0.0", "okn-kg")