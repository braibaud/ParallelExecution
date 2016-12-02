using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Text.RegularExpressions;

namespace ParallelExecution
{
    public class SmtpHelper
    {
        private const int DefaultSmtpPort = 25;
        /// <summary>
        /// 
        /// </summary>
        private string _SmtpServer;

        /// <summary>
        /// Gets or sets the SMTP server.
        /// </summary>
        /// <value>The SMTP server.</value>
        public string SmtpServer
        {
            get
            {
                return _SmtpServer;
            }
            set
            {
                _SmtpServer = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private int _ServerPort;

        /// <summary>
        /// Gets or sets the server port.
        /// </summary>
        /// <value>The server port.</value>
        public int ServerPort
        {
            get
            {
                return _ServerPort;
            }
            set
            {
                _ServerPort = value;
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SmtpHelper"/> class.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <param name="port">The port.</param>
        public SmtpHelper(
            string server,
            int port)
        {
            _SmtpServer = server;
            _ServerPort = port;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SmtpHelper"/> class.
        /// </summary>
        /// <param name="server">The server.</param>
        public SmtpHelper(
            string server)
            : this(server, DefaultSmtpPort)
        {
        }

        /// <summary>
        /// Sends the text mail.
        /// </summary>
        /// <param name="from">From.</param>
        /// <param name="subject">The subject.</param>
        /// <param name="body">The body.</param>
        /// <param name="to">To.</param>
        public void SendTextMail(
            string from,
            string subject,
            string body,
            List<string> to)
        {
            MailMessage message = new MailMessage();

            message.IsBodyHtml = false;

            message.SubjectEncoding = Encoding.UTF8;
            message.Subject = subject;

            message.BodyEncoding = Encoding.UTF8;
            message.Body = body;

            message.From = new MailAddress(from);

            foreach (string address in to)
            {
                message.To.Add(new MailAddress(address));
            }

            SmtpClient scClient = new SmtpClient(
                SmtpServer,
                ServerPort);

            scClient.Credentials = CredentialCache.DefaultNetworkCredentials;
            scClient.Send(message);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="from"></param>
        /// <param name="replyTo"></param>
        /// <param name="fromName"></param>
        /// <param name="recipientsTo"></param>
        /// <param name="recipientsCc"></param>
        /// <param name="recipientsBcc"></param>
        /// <param name="subject"></param>
        /// <param name="textBody"></param>
        /// <param name="htmlBody"></param>
        /// <param name="files"></param>
        public void SendHtmlMail(
            string from,
            string replyTo,
            string fromName,
            List<string> recipientsTo,
            List<string> recipientsCc,
            List<string> recipientsBcc,
            string subject,
            string textBody,
            string htmlBody,
            List<LinkedResource> files)
        {
            if (ValidateEmailAddress(from))
            {
                if (((recipientsTo != null) && (recipientsTo.Count > 0)) ||
                    ((recipientsCc != null) && (recipientsCc.Count > 0)) ||
                    ((recipientsBcc != null) && (recipientsBcc.Count > 0)))
                {
                    MailMessage message = new MailMessage();

                    if (!ValidateEmailAddress(replyTo))
                    {
                        replyTo = from;
                    }

                    if (!string.IsNullOrEmpty(fromName))
                    {
                        message.From = new MailAddress(from, fromName);
                        message.ReplyToList.Add(new MailAddress(replyTo, fromName));
                    }
                    else
                    {
                        message.From = new MailAddress(from);
                        message.ReplyToList.Add(new MailAddress(replyTo));
                    }

                    if (recipientsTo != null)
                    {
                        foreach (string to in recipientsTo)
                        {
                            if (ValidateEmailAddress(to))
                            {
                                message.To.Add(
                                    new MailAddress(
                                        to));
                            }
                            else
                            {
                                throw (new InvalidEmailAddressException(to));
                            }
                        }
                    }

                    if (recipientsCc != null)
                    {
                        foreach (string strCc in recipientsCc)
                        {
                            if (ValidateEmailAddress(strCc))
                            {
                                message.CC.Add(
                                    new MailAddress(
                                        strCc));
                            }
                            else
                            {
                                throw (new InvalidEmailAddressException(strCc));
                            }
                        }
                    }

                    if (recipientsBcc != null)
                    {
                        foreach (string strBcc in recipientsBcc)
                        {
                            if (ValidateEmailAddress(strBcc))
                            {
                                message.Bcc.Add(
                                    new MailAddress(
                                        strBcc));
                            }
                            else
                            {
                                throw (new InvalidEmailAddressException(strBcc));
                            }
                        }
                    }

                    message.IsBodyHtml = true;

                    message.BodyEncoding = Encoding.UTF8;
                    message.Body = textBody;

                    message.SubjectEncoding = Encoding.UTF8;
                    message.Subject = subject;

                    message.Headers.Add(
                        "Errors-To",
                        replyTo);

                    message.Headers.Add(
                        "X-Errors-To",
                        replyTo);

                    AlternateView avHtmlView = new AlternateView(
                        new MemoryStream((new UTF8Encoding()).GetBytes(htmlBody)),
                        System.Net.Mime.MediaTypeNames.Text.Html);

                    foreach (LinkedResource lrFile in files)
                    {
                        avHtmlView.LinkedResources.Add(
                            lrFile);
                    }

                    message.AlternateViews.Add(
                        avHtmlView);

                    SmtpClient scClient = new SmtpClient(
                        SmtpServer,
                        ServerPort);

                    scClient.Credentials = CredentialCache.DefaultNetworkCredentials;
                    scClient.Send(message);
                }
                else
                {
                    throw (new ApplicationException("No recipient specified."));
                }
            }
            else
            {
                throw (new InvalidEmailAddressException(from));
            }
        }

        /// <summary>
        /// Validates the email address.
        /// </summary>
        /// <param name="email">The email.</param>
        /// <returns></returns>
        public static bool ValidateEmailAddress(
            string email)
        {
            bool bRet = false;

            if (!string.IsNullOrEmpty(email))
            {
                bRet = Regex.Match(
                    email,
                    @"^.+@[^\.].*\.[a-z]{2,}$").Success;
            }

            return bRet;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class InvalidEmailAddressException : ApplicationException
    {
        /// <summary>
        /// Gets or sets the invalid email address.
        /// </summary>
        /// <value>The invalid email address.</value>
        public string InvalidEmailAddress
        {
            get;
            set;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidEmailAddressException"/> class.
        /// </summary>
        /// <param name="email">The email.</param>
        public InvalidEmailAddressException(
            string email)
        {
            InvalidEmailAddress = email;
        }
    }
}
