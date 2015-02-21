(function($, undefined) {

Splunk.namespace("Module");
Splunk.Module.NBUsettings = $.klass(Splunk.Module, {

    CALL_TO_ACTION_ADMIN: 'You need to configure this app before it will work!',
    CALL_TO_ACTION_NON_ADMIN:  'Ask your Splunk admin to configure this app before it will work!',

    initialize: function($super, container) {
        $super(container);
        //this.redirectTo = this.getParam('configLink', 'setup');

        this.popup = null;
        this.$popupContainer = $('.FtrPopup', this.container);

        // cache jQ elements
        this.$popupCallToAction = $('.ButtonRow p.callToAction', this.$popupContainer);
        this.$popupConfigureButton = $('.ButtonRow a.configure', this.$popupContainer);
        this.$popupCancelButton = $('.ButtonRow a.cancel', this.$popupContainer);

        // set cancel button click handler
        this.$popupCancelButton.bind('click', function(event) {
            event.preventDefault();
            this.setIgnored();
        }.binwd(this));

        this.getResults();
    },

    renderResults: function(response, turbo) {
        if ((response.has_ignored && response.has_ignored===true) ||
            (response.is_configured && response.is_configured===true)) {
            // already configured or ignored, do nothing
            return true;
        } else if (response.is_admin && response.is_admin===true) {
            // show admin call to action text
            this.$popupCallToAction.html(this.CALL_TO_ACTION_ADMIN);
        } else {
            // show non-admin call to action text and hide configure button
            this.$popupCallToAction.html(this.CALL_TO_ACTION_NON_ADMIN);
            this.$popupConfigureButton.hide();
        }
        // ready to show modal
        this.popup = new Splunk.Popup(this.$popupContainer.get(0), {
            cloneFlag: false,
            pclass: 'configPopup'
        });
    },

    setIgnored: function() {
        var params = this.getResultParams();
        if (!params.hasOwnProperty('client_app')) {
            params['client_app'] = Splunk.util.getCurrentApp();
        }
        params['set_ignore'] = true;
        var xhr = $.ajax({
            type:'GET',
            url: Splunk.util.make_url(
                'module',
                Splunk.util.getConfigValue('SYSTEM_NAMESPACE'),
                this.moduleType,
                'render?' + Splunk.util.propToQueryString(params)),
            beforeSend: function(xhr) {
                xhr.setRequestHeader('X-Splunk-Module', this.moduleType);
            },
            success: function() {
                return true;
            }.bind(this),
            error: function() {
                this.logger.error(_('Unable to set ignored flag'));
            }.bind(this),
            complete: function() {
                this.popup.destroyPopup();
                return true;
            }.bind(this)
        });
    }
});

}(UnixjQuery));