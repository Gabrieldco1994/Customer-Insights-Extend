import "botframework-webchat";

const { createDirectLine, renderWebChat } = globalThis.WebChat;

globalThis.FSIRenegChat = {
  start(session, onStatus) {
    const directLine = createDirectLine({
      token: session.token,
    });
    const subscription = directLine.connectionStatus$.subscribe(onStatus);
    renderWebChat({
      directLine,
      userID: session.userId,
      locale: "pt-BR",
      styleOptions: {
        hideUploadButton: true,
        backgroundColor: "#ffffff",
        accent: "#174d7d",
        botAvatarInitials: "FSI",
        userAvatarInitials: "EU",
      },
    }, document.getElementById("reneg-chat-app"));
    return {
      end() {
        subscription.unsubscribe();
        directLine.end();
      },
    };
  },
};
