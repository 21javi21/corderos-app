/**
 * Hall of Hate Frame Configuration Manager
 * Dynamically applies frame styles to cards based on frame configuration
 */
(function() {
    'use strict';

    // Frame configurations will be embedded by the template
    window.HallOfHateFrames = window.HallOfHateFrames || {};

    function applyFrameStyles(card, frameConfig, frameAsset) {
        if (!card || !frameConfig) return;

        const style = card.style;

        // Apply frame image
        if (frameAsset) {
            style.setProperty('--frame-image', `url('${frameAsset}')`);
        } else {
            style.setProperty('--frame-image', 'none');
        }

        // Apply all frame configuration properties
        const propertyMap = {
            'image_box_top': '--image-box-top',
            'image_box_left': '--image-box-left',
            'image_box_width': '--image-box-width',
            'image_box_height': '--image-box-height',
            'image_frame_top': '--image-frame-top',
            'image_frame_left': '--image-frame-left',
            'image_frame_width': '--image-frame-width',
            'image_frame_height': '--image-frame-height',
            'score_left': '--score-left',
            'score_top': '--score-top',
            'score_width': '--score-width',
            'score_height': '--score-height',
            'score_font_size': '--score-font-size',
            'score_color': '--score-color',
            'score_align': '--score-align',
            'name_top': '--name-top',
            'name_left': '--name-left',
            'name_width': '--name-width',
            'name_align': '--name-align',
            'name_color': '--name-color',
            'name_font_size': '--name-font-size'
        };

        // Set default values
        const defaults = {
            '--score-left': '18%',
            '--score-top': '82%',
            '--score-width': '20%',
            '--score-height': '12%',
            '--score-font-size': 'clamp(1.6rem, 3.6vw, 2.6rem)',
            '--score-color': '#f7fbff',
            '--score-align': 'center',
            '--name-top': '88%',
            '--name-left': '60%',
            '--name-width': '50%',
            '--name-align': 'left',
            '--name-color': '#f7fbff',
            '--name-font-size': 'clamp(0.9rem, 1.6vw, 1.15rem)'
        };

        // Apply frame-specific values or defaults
        Object.entries(propertyMap).forEach(([configKey, cssProperty]) => {
            const value = frameConfig[configKey] || defaults[cssProperty];
            if (value) {
                style.setProperty(cssProperty, value);
            }
        });

        // Handle image frame defaults (fallback to image_box values)
        if (!frameConfig.image_frame_top && frameConfig.image_box_top) {
            style.setProperty('--image-frame-top', frameConfig.image_box_top);
        }
        if (!frameConfig.image_frame_left && frameConfig.image_box_left) {
            style.setProperty('--image-frame-left', frameConfig.image_box_left);
        }
        if (!frameConfig.image_frame_width && frameConfig.image_box_width) {
            style.setProperty('--image-frame-width', frameConfig.image_box_width);
        }
        if (!frameConfig.image_frame_height && frameConfig.image_box_height) {
            style.setProperty('--image-frame-height', frameConfig.image_box_height);
        }
    }

    function applyNameLineClamp(card, nameLines, nameWhiteSpace) {
        const style = card.style;
        style.setProperty('--name-line-clamp', nameLines || '2');
        style.setProperty('--name-white-space', nameWhiteSpace || 'normal');
    }

    function initializeFrameCards() {
        // Read configuration from data attributes
        const configElement = document.getElementById('hall-config');
        if (!configElement) {
            console.error('Hall of Hate: Configuration element not found');
            return;
        }

        try {
            const frames = JSON.parse(configElement.dataset.frames || '{}');
            const frameAssets = JSON.parse(configElement.dataset.frameAssets || '{}');
            
            // Store in global scope for compatibility
            window.HallOfHateFrames = window.HallOfHateFrames || {};
            window.HallOfHateFrames.frames = frames;
            window.HallOfHateFrames.frameAssets = frameAssets;
            
            const frameCards = document.querySelectorAll('.frame-card[data-frame-key]');
            
            frameCards.forEach(card => {
                const frameKey = card.dataset.frameKey;
                const frameAsset = card.dataset.frameAsset;
                const nameLines = card.dataset.nameLines;
                const nameWhiteSpace = card.dataset.nameWhiteSpace;
                
                // Get frame configuration
                const frameConfig = frames[frameKey] || frames['default'] || {};
                
                // Apply styles
                applyFrameStyles(card, frameConfig, frameAsset);
                applyNameLineClamp(card, nameLines, nameWhiteSpace);
            });
        } catch (error) {
            console.error('Hall of Hate: Error parsing configuration', error);
        }
    }

    // Initialize when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initializeFrameCards);
    } else {
        initializeFrameCards();
    }

    // Export for manual usage if needed
    window.HallOfHateFrames.applyFrameStyles = applyFrameStyles;
    window.HallOfHateFrames.initializeFrameCards = initializeFrameCards;
})();